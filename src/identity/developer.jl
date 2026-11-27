# Developer tool credentials: Azure CLI (`az`) and Azure Developer CLI (`azd`).
# Commands are executed directly (never through a shell) with validated
# arguments and a timeout.

"""
Look `names` up in the absolute directories of `PATH`, in `PATH` order.
Unlike `Sys.which`, the current directory is never searched (on Windows
`Sys.which` checks it first), so an `az.cmd` planted in the working directory
of an application cannot be run in place of the real CLI.
"""
function _find_executable(names...)
    for dir in split(get(ENV, "PATH", ""), Sys.iswindows() ? ';' : ':')
        d = Sys.iswindows() ? strip(strip(dir), '"') : dir
        (isempty(d) || !isabspath(d)) && continue
        for n in names
            p = joinpath(d, n)
            isfile(p) && (Sys.iswindows() || Sys.isexecutable(p)) && return p
        end
    end
    return nothing
end

"""
Run `cmd` with a timeout; returns `(exitcode, stdout, stderr)`.
"""
function _run_process(cmd::Cmd, timeout::Real)
    out = IOBuffer()
    err = IOBuffer()
    proc = try
        run(pipeline(ignorestatus(cmd); stdout = out, stderr = err, stdin = devnull); wait = false)
    catch e
        throw(CredentialUnavailableError("failed to start $(cmd.exec[1])"; error = e))
    end
    timed_out = Threads.Atomic{Bool}(false)
    timer = Timer(timeout) do _
        if process_running(proc)
            timed_out[] = true
            kill(proc)
        end
    end
    try
        wait(proc)
    finally
        close(timer)
    end
    timed_out[] && throw(CredentialUnavailableError("$(basename(cmd.exec[1])) timed out after $(timeout) seconds"))
    return proc.exitcode, String(take!(out)), String(take!(err))
end

function _sanitize_stderr(s::AbstractString)
    lines = [strip(l) for l in split(s, '\n') if !isempty(strip(l)) && !startswith(strip(l), "WARNING")]
    msg = join(lines, " ")
    return length(msg) > 500 ? msg[1:prevind(msg, 500)] * "…" : msg
end

"""
    AzureCliCredential(; tenant_id="", additionally_allowed_tenants=String[], process_timeout=10)

Authenticate as the account logged in to the Azure CLI (`az login`).
"""
struct AzureCliCredential <: TokenCredential
    tenant_id::String
    additionally_allowed_tenants::Vector{String}
    process_timeout::Float64
    cache::TokenCache
end
function AzureCliCredential(;
        tenant_id::AbstractString = "", additionally_allowed_tenants = String[],
        process_timeout::Real = 10
    )
    isempty(tenant_id) || validate_tenant_id(tenant_id)
    return AzureCliCredential(
        String(tenant_id), String[String(t) for t in additionally_allowed_tenants],
        Float64(process_timeout), TokenCache()
    )
end
Base.show(io::IO, c::AzureCliCredential) = print(io, "AzureCliCredential(tenant_id=", repr(c.tenant_id), ")")

function AzureCore.get_token_info(
        c::AzureCliCredential, scopes::AbstractString...; claims = nothing,
        tenant_id = nothing, enable_cae::Bool = false
    )
    scopes_v = _require_scopes(scopes)
    length(scopes_v) == 1 || throw(ArgumentError("AzureCliCredential requires exactly one scope per token request"))
    if claims !== nothing && !isempty(claims)
        throw(CredentialUnavailableError("Azure CLI does not support claims challenges; run 'az login --claims-challenge'"))
    end
    resource = scope_to_resource(scopes_v[1])
    tenant = isempty(c.tenant_id) && (tenant_id === nothing || isempty(tenant_id)) ? "" :
        resolve_tenant(
            isempty(c.tenant_id) ? something(tenant_id, "") : c.tenant_id, tenant_id,
            c.additionally_allowed_tenants
        )
    # resolve_tenant returns a requested tenant unchecked when it equals the
    # default; the tenant becomes a command-line argument, so always validate.
    isempty(tenant) || validate_tenant_id(tenant)
    return cached_or_fetch(c.cache, (resource, tenant)) do
        az = _find_executable((Sys.iswindows() ? ("az.cmd", "az.exe") : ("az",))...)
        az === nothing && throw(CredentialUnavailableError("Azure CLI not found on path"))
        args = String[az, "account", "get-access-token", "--output", "json", "--resource", resource]
        isempty(tenant) || append!(args, ["--tenant", tenant])
        cmd = Cmd(Cmd(args); dir = homedir())
        code, out, err = _run_process(cmd, c.process_timeout)
        if code != 0
            e = _sanitize_stderr(err)
            if occursin("az login", e) || occursin("az account set", e) || occursin("AADSTS", e) && occursin("login", e)
                throw(CredentialUnavailableError("Please run 'az login' to set up an account"))
            end
            throw(ClientAuthenticationError("Azure CLI failed to get a token: $e"))
        end
        _parse_cli_token(out)
    end
end

function _parse_cli_token(out::AbstractString)
    obj = try
        JSON.parse(out; dicttype = Dict{String, Any})
    catch
        throw(ClientAuthenticationError("Unexpected output from Azure CLI"))
    end
    token = _json_get(obj, "accessToken")
    token isa AbstractString || throw(ClientAuthenticationError("Unexpected output from Azure CLI: no token"))
    expires_on = _as_int(_json_get(obj, "expires_on"))
    if expires_on === nothing
        s = _json_get(obj, "expiresOn")
        s isa AbstractString || throw(ClientAuthenticationError("Unexpected output from Azure CLI: no expiry"))
        dt = tryparse(Dates.DateTime, replace(s, r"\.\d+$" => ""), Dates.dateformat"y-m-d H:M:S")
        dt === nothing && throw(ClientAuthenticationError("Unexpected output from Azure CLI: bad expiry"))
        # expiresOn is local time
        offset = Dates.value(Dates.now() - Dates.now(Dates.UTC)) / 1000
        expires_on = floor(Int, Dates.datetime2unix(dt) - round(offset / 60) * 60)
    end
    return AccessTokenInfo(String(token), expires_on)
end

"""
    AzureDeveloperCliCredential(; tenant_id="", additionally_allowed_tenants=String[], process_timeout=10)

Authenticate as the account logged in to the Azure Developer CLI (`azd auth login`).
"""
struct AzureDeveloperCliCredential <: TokenCredential
    tenant_id::String
    additionally_allowed_tenants::Vector{String}
    process_timeout::Float64
    cache::TokenCache
end
function AzureDeveloperCliCredential(;
        tenant_id::AbstractString = "", additionally_allowed_tenants = String[],
        process_timeout::Real = 10
    )
    isempty(tenant_id) || validate_tenant_id(tenant_id)
    return AzureDeveloperCliCredential(
        String(tenant_id), String[String(t) for t in additionally_allowed_tenants],
        Float64(process_timeout), TokenCache()
    )
end
Base.show(io::IO, c::AzureDeveloperCliCredential) = print(io, "AzureDeveloperCliCredential(tenant_id=", repr(c.tenant_id), ")")

function AzureCore.get_token_info(
        c::AzureDeveloperCliCredential, scopes::AbstractString...; claims = nothing,
        tenant_id = nothing, enable_cae::Bool = false
    )
    scopes_v = _require_scopes(scopes)
    foreach(validate_scope, scopes_v)
    tenant = isempty(c.tenant_id) && (tenant_id === nothing || isempty(tenant_id)) ? "" :
        resolve_tenant(
            isempty(c.tenant_id) ? something(tenant_id, "") : c.tenant_id, tenant_id,
            c.additionally_allowed_tenants
        )
    isempty(tenant) || validate_tenant_id(tenant)
    return cached_or_fetch(c.cache, (sort(scopes_v), tenant)) do
        azd = _find_executable((Sys.iswindows() ? ("azd.exe",) : ("azd",))...)
        azd === nothing && throw(CredentialUnavailableError("Azure Developer CLI could not be found"))
        args = String[azd, "auth", "token", "--output", "json", "--no-prompt"]
        for s in scopes_v
            append!(args, ["--scope", s])
        end
        isempty(tenant) || append!(args, ["--tenant-id", tenant])
        code, out, err = _run_process(Cmd(Cmd(args); dir = homedir()), c.process_timeout)
        if code != 0
            e = _sanitize_stderr(err)
            if occursin("azd auth login", e) || occursin("not logged in", e)
                throw(CredentialUnavailableError("Please run 'azd auth login' from a command prompt to authenticate before using this credential."))
            end
            throw(ClientAuthenticationError("Azure Developer CLI failed to get a token: $e"))
        end
        obj = try
            JSON.parse(out; dicttype = Dict{String, Any})
        catch
            throw(ClientAuthenticationError("Unexpected output from Azure Developer CLI"))
        end
        token = _json_get(obj, "token")
        exp = _json_get(obj, "expiresOn")
        (token isa AbstractString && exp isa AbstractString) ||
            throw(ClientAuthenticationError("Unexpected output from Azure Developer CLI"))
        dt = tryparse(
            Dates.DateTime, replace(exp, r"Z$" => "", r"\.\d+$" => "", r"[+-]\d\d:\d\d$" => ""),
            Dates.dateformat"y-m-dTH:M:S"
        )
        dt === nothing && throw(ClientAuthenticationError("Unexpected expiry from Azure Developer CLI"))
        AccessTokenInfo(String(token), floor(Int, Dates.datetime2unix(dt)))
    end
end
