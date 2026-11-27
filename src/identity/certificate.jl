# Certificate loading and JWT client-assertion signing via libcrypto
# (OpenSSL_jll). Private keys stay inside OpenSSL and are freed with the
# owning object.

const libcrypto = OpenSSL_jll.libcrypto

mutable struct CertificateKey
    pkey::Ptr{Cvoid}                # EVP_PKEY*
    chain_der::Vector{Vector{UInt8}} # leaf certificate first
    thumbprint_sha1::Vector{UInt8}
    thumbprint_sha256::Vector{UInt8}
    lock::ReentrantLock
    function CertificateKey(pkey::Ptr{Cvoid}, chain::Vector{Vector{UInt8}})
        isempty(chain) && throw(ArgumentError("certificate data contains no certificate"))
        k = new(pkey, chain, SHA.sha1(chain[1]), SHA.sha256(chain[1]), ReentrantLock())
        finalizer(k) do x
            x.pkey == C_NULL || ccall((:EVP_PKEY_free, libcrypto), Cvoid, (Ptr{Cvoid},), x.pkey)
            x.pkey = C_NULL
        end
        return k
    end
end
Base.show(io::IO, k::CertificateKey) = print(io, "CertificateKey(thumbprint=", bytes2hex(k.thumbprint_sha1), ")")

_clear_openssl_errors() = ccall((:ERR_clear_error, libcrypto), Cvoid, ())

function _openssl_error(msg)
    code = ccall((:ERR_get_error, libcrypto), Culong, ())
    _clear_openssl_errors()
    detail = if code == 0
        ""
    else
        buf = Vector{UInt8}(undef, 256)
        ccall((:ERR_error_string_n, libcrypto), Cvoid, (Culong, Ptr{UInt8}, Csize_t), code, buf, length(buf))
        ": " * unsafe_string(pointer(buf))
    end
    return ArgumentError(msg * detail)
end

const _PEM_CERT_RE = r"-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----"s

function _pem_certificates(text::AbstractString)
    ders = Vector{UInt8}[]
    for m in eachmatch(_PEM_CERT_RE, text)
        push!(ders, base64decode(replace(m.captures[1], r"\s" => "")))
    end
    return ders
end

function _pem_private_key(data::Vector{UInt8}, password::Union{Nothing, AbstractString})
    pass = password === nothing ? "" : String(password)
    bio = ccall((:BIO_new_mem_buf, libcrypto), Ptr{Cvoid}, (Ptr{UInt8}, Cint), data, length(data))
    bio == C_NULL && throw(_openssl_error("failed to allocate BIO"))
    try
        # A non-NULL passphrase argument with a NULL callback makes OpenSSL use
        # it directly instead of prompting on the terminal.
        pkey = ccall(
            (:PEM_read_bio_PrivateKey, libcrypto), Ptr{Cvoid}, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Cstring),
            bio, C_NULL, C_NULL, pass
        )
        pkey == C_NULL && throw(_openssl_error("could not load the private key (wrong password or unsupported format)"))
        return pkey
    finally
        ccall((:BIO_free, libcrypto), Cint, (Ptr{Cvoid},), bio)
    end
end

function _x509_der(x509::Ptr{Cvoid})
    n = ccall((:i2d_X509, libcrypto), Cint, (Ptr{Cvoid}, Ptr{Ptr{UInt8}}), x509, C_NULL)
    n <= 0 && throw(_openssl_error("could not encode certificate"))
    buf = Vector{UInt8}(undef, n)
    GC.@preserve buf begin
        p = Ref(pointer(buf))
        ccall((:i2d_X509, libcrypto), Cint, (Ptr{Cvoid}, Ptr{Ptr{UInt8}}), x509, p)
    end
    return buf
end

function _load_pkcs12(data::Vector{UInt8}, password::Union{Nothing, AbstractString})
    p12 = GC.@preserve data begin
        pp = Ref(pointer(data))
        ccall((:d2i_PKCS12, libcrypto), Ptr{Cvoid}, (Ptr{Cvoid}, Ptr{Ptr{UInt8}}, Clong), C_NULL, pp, length(data))
    end
    p12 == C_NULL && throw(_openssl_error("certificate data is neither PEM nor PKCS12"))
    pkey = Ref{Ptr{Cvoid}}(C_NULL)
    cert = Ref{Ptr{Cvoid}}(C_NULL)
    ca = Ref{Ptr{Cvoid}}(C_NULL)
    try
        ok = ccall(
            (:PKCS12_parse, libcrypto), Cint,
            (Ptr{Cvoid}, Cstring, Ptr{Ptr{Cvoid}}, Ptr{Ptr{Cvoid}}, Ptr{Ptr{Cvoid}}),
            p12, password === nothing ? "" : String(password), pkey, cert, ca
        )
        ok == 1 || throw(_openssl_error("could not parse PKCS12 data (wrong password?)"))
        (pkey[] == C_NULL || cert[] == C_NULL) && throw(ArgumentError("PKCS12 data must contain a certificate and its private key"))
        chain = [_x509_der(cert[])]
        if ca[] != C_NULL
            n = ccall((:OPENSSL_sk_num, libcrypto), Cint, (Ptr{Cvoid},), ca[])
            for i in 0:(n - 1)
                x = ccall((:OPENSSL_sk_value, libcrypto), Ptr{Cvoid}, (Ptr{Cvoid}, Cint), ca[], i)
                x == C_NULL || push!(chain, _x509_der(x))
            end
        end
        key = pkey[]
        pkey[] = C_NULL
        return key, chain
    finally
        pkey[] == C_NULL || ccall((:EVP_PKEY_free, libcrypto), Cvoid, (Ptr{Cvoid},), pkey[])
        cert[] == C_NULL || ccall((:X509_free, libcrypto), Cvoid, (Ptr{Cvoid},), cert[])
        if ca[] != C_NULL
            ccall(
                (:OPENSSL_sk_pop_free, libcrypto), Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), ca[],
                cglobal((:X509_free, libcrypto))
            )
        end
        ccall((:PKCS12_free, libcrypto), Cvoid, (Ptr{Cvoid},), p12)
    end
end

const EVP_PKEY_RSA = 6

"""
    load_certificate(data, password=nothing) -> CertificateKey

Load a PEM (certificate + private key) or PKCS#12 blob. Only RSA keys are
supported (as required by Microsoft Entra ID).
"""
function load_certificate(data::Vector{UInt8}, password::Union{Nothing, AbstractString} = nothing)
    _clear_openssl_errors()
    text = String(copy(data))
    pkey, chain = if occursin("-----BEGIN", text)
        ders = _pem_certificates(text)
        isempty(ders) && throw(ArgumentError("PEM data contains no certificate"))
        _pem_private_key(data, password), ders
    else
        _load_pkcs12(data, password)
    end
    keytype = ccall((:EVP_PKEY_get_base_id, libcrypto), Cint, (Ptr{Cvoid},), pkey)
    if keytype != EVP_PKEY_RSA
        ccall((:EVP_PKEY_free, libcrypto), Cvoid, (Ptr{Cvoid},), pkey)
        throw(ArgumentError("only RSA certificates are supported"))
    end
    return CertificateKey(pkey, chain)
end

"Sign `data` with RSASSA-PKCS1-v1_5 / SHA-256 (JWS RS256)."
function sign_rs256(k::CertificateKey, data::Vector{UInt8})
    return @lock k.lock begin
        k.pkey == C_NULL && throw(ArgumentError("certificate key has been freed"))
        ctx = ccall((:EVP_MD_CTX_new, libcrypto), Ptr{Cvoid}, ())
        ctx == C_NULL && throw(_openssl_error("EVP_MD_CTX_new failed"))
        try
            md = ccall((:EVP_sha256, libcrypto), Ptr{Cvoid}, ())
            ccall(
                (:EVP_DigestSignInit, libcrypto), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}),
                ctx, C_NULL, md, C_NULL, k.pkey
            ) == 1 || throw(_openssl_error("EVP_DigestSignInit failed"))
            len = Ref{Csize_t}(0)
            ccall(
                (:EVP_DigestSign, libcrypto), Cint, (Ptr{Cvoid}, Ptr{UInt8}, Ref{Csize_t}, Ptr{UInt8}, Csize_t),
                ctx, C_NULL, len, data, length(data)
            ) == 1 || throw(_openssl_error("EVP_DigestSign failed"))
            sig = Vector{UInt8}(undef, len[])
            ccall(
                (:EVP_DigestSign, libcrypto), Cint, (Ptr{Cvoid}, Ptr{UInt8}, Ref{Csize_t}, Ptr{UInt8}, Csize_t),
                ctx, sig, len, data, length(data)
            ) == 1 || throw(_openssl_error("EVP_DigestSign failed"))
            return resize!(sig, len[])
        finally
            ccall((:EVP_MD_CTX_free, libcrypto), Cvoid, (Ptr{Cvoid},), ctx)
        end
    end
end

base64url(bytes::AbstractVector{UInt8}) = replace(rstrip(base64encode(bytes), '='), '+' => '-', '/' => '_')
base64url(s::AbstractString) = base64url(Vector{UInt8}(codeunits(s)))

"""
    client_assertion_jwt(cert, client_id, audience; send_chain=false, lifetime=600) -> String

Build and sign the JWT client assertion used by the certificate credential.
"""
function client_assertion_jwt(
        k::CertificateKey, client_id::AbstractString, audience::AbstractString;
        send_chain::Bool = false, lifetime::Integer = 600, now::Integer = floor(Int, time())
    )
    header = Dict{String, Any}("alg" => "RS256", "typ" => "JWT", "x5t" => base64url(k.thumbprint_sha1))
    send_chain && (header["x5c"] = [base64encode(d) for d in k.chain_der])
    payload = Dict{String, Any}(
        "aud" => audience, "iss" => client_id, "sub" => client_id,
        "jti" => string(uuid4()), "nbf" => now, "iat" => now, "exp" => now + lifetime
    )
    signing_input = string(base64url(JSON.json(header)), ".", base64url(JSON.json(payload)))
    sig = sign_rs256(k, Vector{UInt8}(codeunits(signing_input)))
    return string(signing_input, ".", base64url(sig))
end
