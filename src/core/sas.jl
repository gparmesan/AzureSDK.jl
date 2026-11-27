# Shared access signatures and connection strings.

"""
    generate_sas_token(uri, key_name, key; expiry=3600) -> (token::String, expires_on::Int)

Create a Service Bus / Event Hubs shared access signature for resource `uri`
(e.g. `"sb://myns.servicebus.windows.net/myhub"`) valid for `expiry` seconds.
"""
function generate_sas_token(
        uri::AbstractString, key_name::AbstractString, key::AbstractString;
        expiry::Real = 3600, now::Real = time()
    )
    expires_on = floor(Int, now + expiry)
    encoded_uri = URIs.escapeuri(String(uri))
    to_sign = string(encoded_uri, "\n", expires_on)
    sig = base64encode(hmac_sha256(Vector{UInt8}(codeunits(String(key))), to_sign))
    token = string(
        "SharedAccessSignature sr=", encoded_uri, "&sig=", URIs.escapeuri(sig), "&se=", expires_on,
        "&skn=", URIs.escapeuri(String(key_name))
    )
    return token, expires_on
end

"""
    sas_expiry(signature) -> Union{Int,Nothing}

Extract the `se=` expiry (Unix seconds) from a shared access signature.
"""
function sas_expiry(signature::AbstractString)
    m = match(r"(?:^|[&\s])se=(\d+)", signature)
    m === nothing && return nothing
    return tryparse(Int, m.captures[1])
end

"""
    parse_connection_string_pairs(conn_str) -> Dict{String,String}

Split a `Key=Value;Key=Value` connection string. Keys are lower-cased; values
may contain `=`. Throws `ArgumentError` for malformed or duplicate entries.
"""
function parse_connection_string_pairs(conn_str::AbstractString)
    d = Dict{String, String}()
    for part in split(strip(conn_str), ';')
        part = strip(part)
        isempty(part) && continue
        i = findfirst('=', part)
        i === nothing && throw(ArgumentError("Connection string is either blank or malformed."))
        k = lowercase(strip(part[1:(i - 1)]))
        v = strip(part[(i + 1):end])
        (isempty(k) || isempty(v)) && throw(ArgumentError("Connection string is either blank or malformed."))
        haskey(d, k) && throw(ArgumentError("Connection string contains duplicate key: $(part[1:(i - 1)])"))
        d[k] = String(v)
    end
    isempty(d) && throw(ArgumentError("Connection string is either blank or malformed."))
    return d
end
