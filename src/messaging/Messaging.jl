"""
    AzureSDK.Messaging

Internal infrastructure shared by `AzureSDK.EventHubs` and
`AzureSDK.ServiceBus`: transport selection, connection strings, credentials
to CBS token providers, AMQP error classification and the retry loop.
"""
module Messaging

using Dates
import HTTP
import Reseau
import URIs

using ..AzureCore
using ..AzureCore: retry_options, backoff_delay, sas_expiry, signature
using ..AMQP
using ..AMQP: AMQPException, ClientConnection, TransportConfig, ClientClosedError

export TransportType, AmqpAnnotatedMessage

"""
    AmqpAnnotatedMessage

Alias of `AzureSDK.AMQP.AMQPMessage` (Python: `azure.core.amqp.AmqpAnnotatedMessage`):
a raw AMQP message with full control over header, annotations, properties and
body. Can be sent with `send_messages`, `add_message!`, `send_event` and `add!`.
"""
const AmqpAnnotatedMessage = AMQP.AMQPMessage

# Generic functions shared by EventHubs and ServiceBus (both add methods), so the
# two modules can be `using`-ed together without export conflicts.
import ..AzureCore: from_connection_string
"Message/event body decoded as a UTF-8 string."
function body_as_str end
"Current encoded size of a batch in bytes."
function size_in_bytes end
"Maximum encoded size of a batch in bytes."
function max_size_in_bytes end

"""
    TransportType.Amqp / TransportType.AmqpOverWebsocket / TransportType.Http

How a client talks to the service:

- `Amqp`: AMQP 1.0 over TLS on port 5671 (default; best performance).
- `AmqpOverWebsocket`: AMQP 1.0 tunnelled through a WebSocket on port 443
  (works through firewalls/proxies that only allow HTTPS; supports `http_proxy`).
- `Http`: the services' REST APIs over HTTPS. Only a subset of operations is
  available (sending, and for Service Bus basic receive/settle); this is an
  extension not present in the Python SDK.
"""
module TransportType
    @enum T Amqp AmqpOverWebsocket Http
end

# ---------------------------------------------------------------------------
# Connection strings
# ---------------------------------------------------------------------------

"""
Parsed Event Hubs / Service Bus connection string.
"""
struct ConnectionStringProperties
    fully_qualified_namespace::String
    endpoint::String
    entity_path::Union{Nothing, String}
    shared_access_key_name::Union{Nothing, String}
    shared_access_key::Union{Nothing, String}
    shared_access_signature::Union{Nothing, String}
    use_development_emulator::Bool
    port::Union{Nothing, Int}
end

function Base.show(io::IO, p::ConnectionStringProperties)
    return print(
        io, "ConnectionStringProperties(fully_qualified_namespace=", repr(p.fully_qualified_namespace),
        ", entity_path=", repr(p.entity_path), ", shared_access_key_name=", repr(p.shared_access_key_name),
        p.shared_access_key === nothing ? "" : ", shared_access_key=<redacted>",
        p.shared_access_signature === nothing ? "" : ", shared_access_signature=<redacted>",
        p.use_development_emulator ? ", use_development_emulator=true" : "", ")"
    )
end

# The Event Hubs / Service Bus emulators' publicly documented key (not a secret).
const EMULATOR_SAS_KEY = "SAS_KEY_VALUE"
const _PLAINTEXT_KEY_MSG = "the development emulator is reached without TLS: on a host other than a loopback \
    host (e.g. a docker-compose service name) only the emulator's public key (SharedAccessKey=$(EMULATOR_SAS_KEY)) \
    may be used"

"""
    plaintext_credential_ok(host, credential) -> Bool

Whether `credential` may be used over an unencrypted connection to `host`:
anything on a loopback host, elsewhere (single-label emulator host names, which
may resolve through the DNS search domain to any machine) only the emulators'
public key, so a real key or token is never sent in clear text.
"""
plaintext_credential_ok(host::AbstractString, credential) =
    AzureCore.is_local_host(host) ||
    credential isa AzureNamedKeyCredential && named_key(credential).key == EMULATOR_SAS_KEY

function _check_plaintext_credential(cfg)
    cfg.use_emulator && !plaintext_credential_ok(cfg.fully_qualified_namespace, cfg.credential) &&
        throw(ArgumentError(_PLAINTEXT_KEY_MSG))
    return nothing
end

function parse_messaging_connection_string(conn_str::AbstractString)
    d = parse_connection_string_pairs(conn_str)
    endpoint = get(d, "endpoint", nothing)
    endpoint === nothing && throw(ArgumentError("Connection string is either blank or malformed."))
    u = URIs.URI(occursin("://", endpoint) ? endpoint : "sb://" * endpoint)
    isempty(u.host) && throw(ArgumentError("Invalid endpoint in connection string"))
    port = isempty(u.port) ? nothing : parse(Int, u.port)
    key_name = get(d, "sharedaccesskeyname", nothing)
    key = get(d, "sharedaccesskey", nothing)
    sas = get(d, "sharedaccesssignature", nothing)
    if sas !== nothing && (key_name !== nothing || key !== nothing)
        throw(ArgumentError("Only one of the SharedAccessKey or SharedAccessSignature must be present."))
    end
    if sas === nothing && (key_name === nothing || key === nothing)
        throw(ArgumentError("Invalid connection string: SharedAccessKeyName and SharedAccessKey are required (or SharedAccessSignature)."))
    end
    emulator = lowercase(get(d, "usedevelopmentemulator", "false")) == "true"
    if emulator && !AzureCore.is_dev_host(u.host)
        throw(ArgumentError("UseDevelopmentEmulator may only be used with a local endpoint"))
    end
    if emulator && !AzureCore.is_local_host(u.host) && (sas !== nothing || key != EMULATOR_SAS_KEY)
        throw(ArgumentError(_PLAINTEXT_KEY_MSG))
    end
    entity = get(d, "entitypath", nothing)
    return ConnectionStringProperties(
        u.host, string("sb://", u.host, isempty(u.port) ? "" : ":" * u.port, "/"),
        entity, key_name, key, sas, emulator, port
    )
end

"Credential object for the secret in a parsed connection string."
function connection_string_credential(p::ConnectionStringProperties)
    p.shared_access_signature !== nothing && return AzureSasCredential(p.shared_access_signature)
    return AzureNamedKeyCredential(p.shared_access_key_name, p.shared_access_key)
end

# ---------------------------------------------------------------------------
# Credentials -> CBS token providers / HTTP authorization headers
# ---------------------------------------------------------------------------

const SAS_TOKEN_TYPE = "servicebus.windows.net:sastoken"
const JWT_TOKEN_TYPE = "jwt"

"""
    token_provider(credential, scope) -> (audience -> (token, token_type, expires_on))
"""
function token_provider(credential, scope::AbstractString; sas_ttl::Real = 3600)
    if credential isa AzureNamedKeyCredential
        return function (audience)
            nk = named_key(credential)
            tok, exp = generate_sas_token(audience, nk.name, nk.key; expiry = sas_ttl)
            return tok, SAS_TOKEN_TYPE, Float64(exp)
        end
    elseif credential isa AzureSasCredential
        return function (audience)
            sig = signature(credential)
            exp = something(sas_expiry(sig), floor(Int, time()) + 3600)
            return sig, SAS_TOKEN_TYPE, Float64(exp)
        end
    elseif credential isa TokenCredential
        s = String(scope)
        return function (audience)
            t = get_token(credential, s)
            return t.token, JWT_TOKEN_TYPE, Float64(t.expires_on)
        end
    end
    throw(ArgumentError("unsupported credential type $(typeof(credential)); expected a TokenCredential, AzureNamedKeyCredential or AzureSasCredential"))
end

"Value for the HTTP `Authorization` header."
function http_authorization(credential, audience::AbstractString, scope::AbstractString)
    tok, typ, _ = token_provider(credential, scope)(audience)
    return typ == JWT_TOKEN_TYPE ? "Bearer " * tok : tok
end

# ---------------------------------------------------------------------------
# Client configuration
# ---------------------------------------------------------------------------

"""
Transport/retry configuration shared by all messaging clients.
"""
struct ClientConfig
    fully_qualified_namespace::String
    credential::Any
    scope::String
    transport_type::TransportType.T
    port::Union{Nothing, Int}
    use_emulator::Bool
    custom_endpoint_address::Union{Nothing, String}
    http_proxy::Any
    connection_verify::Union{Bool, String}
    retry::RetryOptions
    auth_timeout::Float64
    user_agent::String
    component::String
    scheme_override::Union{Nothing, Symbol}
    http_client::Any          # HTTP.Client with custom TLS/proxy settings, or nothing
end

function Base.show(io::IO, c::ClientConfig)
    return print(io, "ClientConfig(", c.fully_qualified_namespace, ", ", c.transport_type, ")")
end

function _transport_type(t)
    t isa TransportType.T && return t
    s = lowercase(string(t))
    s in ("amqp",) && return TransportType.Amqp
    s in ("amqpoverwebsocket", "websocket", "websockets") && return TransportType.AmqpOverWebsocket
    s == "http" && return TransportType.Http
    throw(ArgumentError("invalid transport_type $(t)"))
end

function _normalize_namespace(ns::AbstractString)
    s = strip(ns)
    for prefix in ("sb://", "amqps://", "amqp://", "https://", "http://")
        startswith(lowercase(s), prefix) && (s = s[(length(prefix) + 1):end])
    end
    s = rstrip(s, '/')
    occursin('/', s) && throw(ArgumentError("fully_qualified_namespace must be a host name, e.g. 'myns.servicebus.windows.net'"))
    isempty(s) && throw(ArgumentError("fully_qualified_namespace must not be empty"))
    return String(s)
end

function ClientConfig(
        fully_qualified_namespace::AbstractString, credential, scope::AbstractString, component::AbstractString;
        transport_type = TransportType.Amqp, port = nothing, use_development_emulator::Bool = false,
        custom_endpoint_address = nothing, http_proxy = nothing, connection_verify = true,
        retry_total::Integer = 3, retry_backoff_factor::Real = 0.8, retry_backoff_max::Real = 120.0,
        retry_mode = RetryMode.Exponential, auth_timeout::Real = 60.0, user_agent = nothing, _scheme = nothing
    )
    ns = _normalize_namespace(fully_qualified_namespace)
    host = first(split(ns, ':'))
    if occursin(':', ns) && port === nothing
        port = parse(Int, split(ns, ':')[2])
    end
    tt = _transport_type(transport_type)
    if use_development_emulator
        # The emulator is reached over unencrypted AMQP/HTTP.
        AzureCore.is_dev_host(host) ||
            throw(ArgumentError("use_development_emulator may only be used with a local endpoint"))
        plaintext_credential_ok(host, credential) || throw(ArgumentError(_PLAINTEXT_KEY_MSG))
    end
    # private test hook: only the in-process loopback peer, never a plaintext network transport
    _scheme === nothing || Symbol(_scheme) === :loopback ||
        throw(ArgumentError("_scheme only accepts :loopback"))
    use_development_emulator && tt == TransportType.AmqpOverWebsocket &&
        throw(ArgumentError("the development emulator does not support AMQP over WebSockets"))
    http_proxy !== nothing && tt == TransportType.Amqp &&
        throw(ArgumentError("http_proxy requires transport_type=TransportType.AmqpOverWebsocket"))
    connection_verify === false &&
        @warn "TLS certificate verification is disabled (connection_verify=false); never do this in production"
    ua = AzureCore.user_agent(component)
    user_agent === nothing || (ua = string(user_agent, " ", ua))
    return ClientConfig(
        String(host), credential, String(scope), tt, port, use_development_emulator,
        custom_endpoint_address === nothing ? nothing : String(custom_endpoint_address), http_proxy,
        connection_verify isa AbstractString ? String(connection_verify) : Bool(connection_verify),
        retry_options(; retry_total, retry_backoff_factor, retry_backoff_max, retry_mode), Float64(auth_timeout),
        ua, String(component), _scheme === nothing ? nothing : Symbol(_scheme),
        _http_client(connection_verify, http_proxy)
    )
end

"HTTP client honouring `connection_verify` (CA bundle path or `false`) and `http_proxy`."
_http_client(connection_verify, http_proxy) = AzureCore.http_client(connection_verify, http_proxy)

"Create the (lazily connecting) AMQP connection for a client."
function make_connection(cfg::ClientConfig)
    cfg.transport_type == TransportType.Http && throw(ArgumentError("AMQP connection requested for an HTTP client"))
    scheme = cfg.scheme_override !== nothing ? cfg.scheme_override :
        cfg.use_emulator ? :amqp :
        cfg.transport_type == TransportType.AmqpOverWebsocket ? :wss : :amqps
    tc = TransportConfig(
        scheme = scheme, host = cfg.fully_qualified_namespace, port = cfg.port,
        verify_tls = cfg.connection_verify !== false,
        ca_file = cfg.connection_verify isa String ? cfg.connection_verify : nothing,
        http_proxy = cfg.http_proxy, custom_endpoint_address = cfg.custom_endpoint_address,
        connect_timeout = cfg.auth_timeout, http_client = cfg.http_client
    )
    props = Dict{Symbol, Any}(
        :product => "azsdk-julia-$(cfg.component)", :version => AzureCore.PACKAGE_VERSION,
        :framework => "Julia/$(VERSION)", Symbol("user-agent") => cfg.user_agent
    )
    return ClientConnection(
        tc; hostname = cfg.fully_qualified_namespace, sasl_mechanism = :anonymous,
        properties = props, token_provider = _checked_token_provider(cfg),
        operation_timeout = cfg.auth_timeout
    )
end

function _checked_token_provider(cfg::ClientConfig)
    provider = token_provider(cfg.credential, cfg.scope)
    cfg.use_emulator || return provider
    # re-check on every token: the credential may have been rotated with update!
    return audience -> (_check_plaintext_credential(cfg); provider(audience))
end

"`Authorization` header value for a client; refuses a real key on an unencrypted emulator connection."
function http_authorization(cfg::ClientConfig, audience::AbstractString, scope::AbstractString = cfg.scope)
    _check_plaintext_credential(cfg)      # the credential may have been rotated since the client was created
    return http_authorization(cfg.credential, audience, scope)
end

"`sb://host/path` resource URI used as CBS audience and SAS resource."
resource_uri(cfg::ClientConfig, path::AbstractString) = string("sb://", cfg.fully_qualified_namespace, "/", path)

"Base HTTPS URL for REST calls."
function https_base(cfg::ClientConfig)
    host = cfg.fully_qualified_namespace
    port = cfg.port === nothing ? "" : ":$(cfg.port)"
    return cfg.use_emulator ? "http://$(host)$(port)" : "https://$(host)$(port)"
end

# ---------------------------------------------------------------------------
# Error classification and retries
# ---------------------------------------------------------------------------

const RETRIABLE_CONDITIONS = Set(
    Symbol.(
        [
            "amqp:internal-error", "amqp:connection:forced", "amqp:connection:framing-error",
            "amqp:link:detach-forced", "amqp:session:window-violation", "amqp:session:errant-link",
            "amqp:socket-error", "amqp:closed", "amqp:resource-limit-exceeded",
            "com.microsoft:server-busy", "com.microsoft:timeout", "com.microsoft:operation-cancelled",
            "com.microsoft:container-close", "com.microsoft:store-lock-lost",
            "amqp:link:redirect", "amqp:connection:redirect", "com.microsoft:partition-not-owned",
        ]
    )
)

"""
    error_kind(err) -> Symbol

Classify an exception: `:auth`, `:not_found`, `:timeout`, `:connection`,
`:link`, `:message_size`, `:server_busy`, `:quota`, `:argument`,
`:link_stolen`, `:closed`, `:entity_disabled`, `:lock_lost`,
`:session_lock_lost`, `:session_cannot_be_locked`, `:message_not_found`,
`:precondition` or `:other`.
"""
function error_kind(err)
    err isa ClientClosedError && return :closed
    if !(err isa AMQPException)
        _is_certificate_error(err) && return :tls
        return _is_transport_error(err) ? :connection : :other
    end
    c = String(err.condition)
    c == "amqp:unauthorized-access" && return :auth
    c in ("amqp:not-found", "com.microsoft:entity-not-found", "amqp:resource-deleted") && return :not_found
    c == "com.microsoft:timeout" && return :timeout
    c == "amqp:link:message-size-exceeded" && return :message_size
    c == "com.microsoft:server-busy" && return :server_busy
    c == "amqp:resource-limit-exceeded" && return :quota
    c in (
        "com.microsoft:argument-error", "com.microsoft:argument-out-of-range", "amqp:invalid-field",
        "amqp:not-implemented", "amqp:not-allowed", "amqp:decode-error",
    ) && return :argument
    c == "amqp:link:stolen" && return :link_stolen
    c == "com.microsoft:entity-disabled" && return :entity_disabled
    c == "com.microsoft:message-lock-lost" && return :lock_lost
    c == "com.microsoft:session-lock-lost" && return :session_lock_lost
    c == "com.microsoft:session-cannot-be-locked" && return :session_cannot_be_locked
    c == "com.microsoft:message-not-found" && return :message_not_found
    c == "com.microsoft:precondition-failed" && return :precondition
    err.scope in (:transport, :connection, :protocol) && return :connection
    err.scope in (:link, :session) && return :link
    return :other
end

# TLS certificate problems are configuration errors: never retry them.
_is_certificate_error(err) = err isa Exception && nameof(typeof(err)) in (:TLSError, :TLSHandshakeError) &&
    occursin(r"certificate|x509|verif"i, sprint(showerror, err))

# Failures to reach or keep talking to the endpoint (DNS, TCP, TLS, WebSocket).
function _is_transport_error(err)
    (err isa Base.IOError || err isa EOFError || err isa ServiceRequestError) && return true
    n = nameof(typeof(err))
    return n in (
        :TLSError, :TLSHandshakeError, :TLSHandshakeTimeoutError, :DNSError, :DeadlineExceededError,
        :ConnectError, :WebSocketError, :SystemError, :OpError, :AddrError,
    )
end

"Whether an error may succeed when retried."
function is_retriable(err)
    err isa AzureError && hasproperty(err, :retriable) && return getproperty(err, :retriable)
    _is_certificate_error(err) && return false
    err isa AMQPException || return _is_transport_error(err)
    err.condition in RETRIABLE_CONDITIONS && return true
    return error_kind(err) in (:connection, :link, :timeout, :server_busy)
end

"""
    run_with_retry(f, retry, is_closed; map_error=identity, operation="operation", deadline=nothing)

Run `f()`; on retriable failures back off and try again, up to
`retry.retry_total` retries. The final error is passed through `map_error`.
"""
function run_with_retry(
        f, retry::RetryOptions, is_closed::Function; map_error = identity,
        operation::AbstractString = "operation", deadline::Union{Nothing, Float64} = nothing
    )
    attempt = 0
    while true
        try
            return f()
        catch err
            err isa InterruptException && rethrow()
            if is_closed() || !is_retriable(err) || attempt >= retry.retry_total
                throw(map_error(err))
            end
            attempt += 1
            delay = backoff_delay(retry, attempt)
            if error_kind(err) === :server_busy
                delay = max(delay, 4.0)   # the service asks for at least a few seconds
            end
            if deadline !== nothing && time() + delay >= deadline
                throw(map_error(err))
            end
            @debug "$(operation) failed; retrying" attempt delay exception = err
            sleep(delay)
        end
    end
    return
end

end # module
