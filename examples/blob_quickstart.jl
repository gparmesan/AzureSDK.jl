# Azure Blob Storage quick start.
#
#   docker compose -f test/emulator/azurite-compose.yml up -d     # local Azurite
#   julia --project examples/blob_quickstart.jl
#
# Against a real account set AZURE_STORAGE_CONNECTION_STRING, or replace the
# client construction with
#   BlobServiceClient("https://<account>.blob.core.windows.net"; credential=DefaultAzureCredential())

using AzureSDK, AzureSDK.Storage.Blob
using Dates

svc = from_connection_string(BlobServiceClient, get(ENV, "AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true"))
container = get_container_client(svc, "quickstart")
exists(container) || create_container(container)

# Upload a string, bytes and a (larger) stream in parallel blocks
upload_blob(container, "hello.txt", "Hello, Blob Storage!"; overwrite=true,
    content_settings=ContentSettings(content_type="text/plain"), metadata=Dict("source" => "quickstart"))
data = rand(UInt8, 20 * 1024 * 1024)
upload_blob(container, "random.bin", IOBuffer(data); overwrite=true, max_concurrency=4)

# Download
println(content_as_text(download_blob(container, "hello.txt")))
@assert readall(download_blob(container, "random.bin"; max_concurrency=4)) == data

# Properties and listing
props = get_blob_properties(get_blob_client(container, "hello.txt"))
println("hello.txt: ", props.size, " bytes, ", props.content_settings.content_type, ", metadata ", props.metadata)
for b in list_blobs(container; include=["metadata"])
    println(" - ", b.name, " (", b.size, " bytes)")
end

# A read-only link valid for one hour (account key credentials only)
cred = svc.credential
if cred isa AzureNamedKeyCredential
    key = AzureSDK.AzureCore.named_key(cred)
    sas = generate_blob_sas(key.name, container.container_name, "hello.txt"; account_key=key.key,
        permission=BlobSasPermissions(read=true), expiry=Dates.Hour(1))
    println("SAS URL: ", get_blob_client(container, "hello.txt").url, "?", sas)
end

# Clean up
delete_container(container)
