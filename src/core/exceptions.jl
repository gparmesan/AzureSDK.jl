# Exception hierarchy (azure.core.exceptions).
#
# Julia exceptions cannot inherit from concrete types, so the Python class
# hierarchy is expressed with abstract types:
#
#   AzureError
#   ├── ServiceRequestError          (request could not be sent)
#   ├── ServiceResponseError         (no/invalid response)
#   └── AbstractHttpResponseError
#       ├── HttpResponseError
#       ├── ClientAuthenticationError
#       ├── ResourceNotFoundError
#       ├── ResourceExistsError
#       ├── ResourceModifiedError
#       └── ResourceNotModifiedError

"""
    AzureError

Abstract supertype of all errors raised by this package. Every concrete error
has at least the fields `message::String` and `error` (the underlying
exception, or `nothing`).
"""
abstract type AzureError <: Exception end

function Base.showerror(io::IO, e::AzureError)
    print(io, nameof(typeof(e)), ": ", e.message)
    inner = hasfield(typeof(e), :error) ? getfield(e, :error) : nothing
    return if inner !== nothing
        print(io, "\ncaused by: ")
        inner isa Exception ? showerror(io, inner) : print(io, inner)
    end
end

"""
    @azure_error Name [Parent]

Define a concrete error type `Name <: Parent` (default `AzureError`) with fields
`message` and `error`, constructible as `Name(message; error=nothing)`.
"""
macro azure_error(name, parent = :AzureError)
    return esc(
        quote
            struct $name <: $parent
                message::String
                error::Any
            end
            $name(message::AbstractString = $(string(name)); error = nothing) = $name(String(message), error)
        end
    )
end

@azure_error ServiceRequestError
@azure_error ServiceResponseError
@azure_error IncompleteReadError

abstract type AbstractHttpResponseError <: AzureError end

for T in (
        :HttpResponseError, :ClientAuthenticationError, :ResourceNotFoundError, :ResourceExistsError,
        :ResourceModifiedError, :ResourceNotModifiedError,
    )
    @eval begin
        """
            $($(string(T)))(message; status_code=nothing, reason=nothing, response_body=nothing, error_code=nothing,
                             error=nothing)

        `error_code` is the service's error code (e.g. `"BlobNotFound"`), when known.
        """
        struct $T <: AbstractHttpResponseError
            message::String
            status_code::Union{Nothing, Int}
            reason::Union{Nothing, String}
            response_body::Union{Nothing, String}
            error_code::Union{Nothing, String}
            error::Any
        end
        $T(
            message::AbstractString = $(string(T)); status_code = nothing, reason = nothing, response_body = nothing,
            error_code = nothing, error = nothing
        ) =
            $T(
            String(message), status_code, reason, response_body,
            error_code === nothing ? nothing : String(error_code), error
        )
    end
end

function Base.showerror(io::IO, e::AbstractHttpResponseError)
    print(io, nameof(typeof(e)), ": ", e.message)
    e.status_code === nothing || print(io, " (status ", e.status_code, e.reason === nothing ? "" : " " * e.reason, ")")
    e.error_code === nothing || print(io, " [", e.error_code, "]")
    return if e.error !== nothing
        print(io, "\ncaused by: ")
        e.error isa Exception ? showerror(io, e.error) : print(io, e.error)
    end
end
