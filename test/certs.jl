# Generates throw-away self-signed certificates for the tests with libcrypto
# (no key material is stored in the repository).

module TestCerts

import OpenSSL_jll
const lib = OpenSSL_jll.libcrypto

check(x, what) = (x == C_NULL || x == 0) ? error("OpenSSL call failed: $what") : x

function rsa_key(bits=2048)
    ctx = check(ccall((:EVP_PKEY_CTX_new_id, lib), Ptr{Cvoid}, (Cint, Ptr{Cvoid}), 6, C_NULL), "ctx")
    try
        check(ccall((:EVP_PKEY_keygen_init, lib), Cint, (Ptr{Cvoid},), ctx) == 1 ? 1 : 0, "keygen_init")
        check(ccall((:EVP_PKEY_CTX_set_rsa_keygen_bits, lib), Cint, (Ptr{Cvoid}, Cint), ctx, bits) == 1 ? 1 : 0, "bits")
        pk = Ref{Ptr{Cvoid}}(C_NULL)
        check(ccall((:EVP_PKEY_keygen, lib), Cint, (Ptr{Cvoid}, Ptr{Ptr{Cvoid}}), ctx, pk) == 1 ? 1 : 0, "keygen")
        return pk[]
    finally
        ccall((:EVP_PKEY_CTX_free, lib), Cvoid, (Ptr{Cvoid},), ctx)
    end
end

function bio_string(bio)
    p = Ref{Ptr{UInt8}}(C_NULL)
    n = ccall((:BIO_ctrl, lib), Clong, (Ptr{Cvoid}, Cint, Clong, Ptr{Ptr{UInt8}}), bio, 3, 0, p)  # BIO_CTRL_INFO
    return unsafe_string(p[], n)
end

function bio_bytes(bio)
    p = Ref{Ptr{UInt8}}(C_NULL)
    n = ccall((:BIO_ctrl, lib), Clong, (Ptr{Cvoid}, Cint, Clong, Ptr{Ptr{UInt8}}), bio, 3, 0, p)
    return copy(unsafe_wrap(Array, p[], n))
end

new_mem_bio() = check(ccall((:BIO_new, lib), Ptr{Cvoid}, (Ptr{Cvoid},), ccall((:BIO_s_mem, lib), Ptr{Cvoid}, ())), "BIO_new")
free_bio(b) = ccall((:BIO_free, lib), Cint, (Ptr{Cvoid},), b)

"""
    self_signed(cn="localhost"; days=1, password=nothing) -> (cert_pem, key_pem, pkcs12_der)

Create an RSA key and a self-signed certificate (with SAN DNS:localhost,
IP:127.0.0.1). With `password`, the PEM key is encrypted and the PKCS#12
blob protected with it.
"""
function self_signed(cn::AbstractString="localhost"; days::Integer=1, password=nothing)
    pkey = rsa_key()
    x = check(ccall((:X509_new, lib), Ptr{Cvoid}, ()), "X509_new")
    try
        ccall((:X509_set_version, lib), Cint, (Ptr{Cvoid}, Clong), x, 2)
        serial = ccall((:X509_get_serialNumber, lib), Ptr{Cvoid}, (Ptr{Cvoid},), x)
        ccall((:ASN1_INTEGER_set, lib), Cint, (Ptr{Cvoid}, Clong), serial, rand(1:typemax(Int32)))
        ccall((:X509_gmtime_adj, lib), Ptr{Cvoid}, (Ptr{Cvoid}, Clong),
            ccall((:X509_getm_notBefore, lib), Ptr{Cvoid}, (Ptr{Cvoid},), x), -3600)
        ccall((:X509_gmtime_adj, lib), Ptr{Cvoid}, (Ptr{Cvoid}, Clong),
            ccall((:X509_getm_notAfter, lib), Ptr{Cvoid}, (Ptr{Cvoid},), x), days * 86400)
        ccall((:X509_set_pubkey, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}), x, pkey)
        name = ccall((:X509_get_subject_name, lib), Ptr{Cvoid}, (Ptr{Cvoid},), x)
        ccall((:X509_NAME_add_entry_by_txt, lib), Cint, (Ptr{Cvoid}, Cstring, Cint, Cstring, Cint, Cint, Cint),
            name, "CN", 0x1001, cn, -1, -1, 0)
        ccall((:X509_set_issuer_name, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}), x, name)
        for (nid, value) in ((85, "DNS:$(cn),IP:127.0.0.1"), (87, "critical,CA:TRUE"))
            ext = check(ccall((:X509V3_EXT_conf_nid, lib), Ptr{Cvoid}, (Ptr{Cvoid}, Ptr{Cvoid}, Cint, Cstring),
                C_NULL, C_NULL, nid, value), "ext")
            ccall((:X509_add_ext, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Cint), x, ext, -1)
            ccall((:X509_EXTENSION_free, lib), Cvoid, (Ptr{Cvoid},), ext)
        end
        md = ccall((:EVP_sha256, lib), Ptr{Cvoid}, ())
        ccall((:X509_sign, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}), x, pkey, md) > 0 || error("X509_sign failed")

        bio = new_mem_bio()
        ccall((:PEM_write_bio_X509, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}), bio, x)
        cert_pem = bio_string(bio)
        free_bio(bio)

        bio = new_mem_bio()
        if password === nothing
            ccall((:PEM_write_bio_PrivateKey, lib), Cint,
                (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Cint, Ptr{Cvoid}, Ptr{Cvoid}),
                bio, pkey, C_NULL, C_NULL, 0, C_NULL, C_NULL)
        else
            cipher = ccall((:EVP_aes_256_cbc, lib), Ptr{Cvoid}, ())
            ccall((:PEM_write_bio_PKCS8PrivateKey, lib), Cint,
                (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Cint, Ptr{Cvoid}, Cstring),
                bio, pkey, cipher, C_NULL, 0, C_NULL, password)
        end
        key_pem = bio_string(bio)
        free_bio(bio)

        p12 = check(ccall((:PKCS12_create, lib), Ptr{Cvoid},
                (Cstring, Cstring, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Cint, Cint, Cint, Cint, Cint),
                something(password, ""), "test", pkey, x, C_NULL, 0, 0, 0, 0, 0), "PKCS12_create")
        bio = new_mem_bio()
        ccall((:i2d_PKCS12_bio, lib), Cint, (Ptr{Cvoid}, Ptr{Cvoid}), bio, p12)
        p12_der = bio_bytes(bio)
        free_bio(bio)
        ccall((:PKCS12_free, lib), Cvoid, (Ptr{Cvoid},), p12)
        return cert_pem, key_pem, p12_der
    finally
        ccall((:X509_free, lib), Cvoid, (Ptr{Cvoid},), x)
        ccall((:EVP_PKEY_free, lib), Cvoid, (Ptr{Cvoid},), pkey)
    end
end

end # module
