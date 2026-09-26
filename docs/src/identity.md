# Authentication

```julia
using AzureSDK.Identity

cred = DefaultAzureCredential()   # environment → workload identity → managed identity → az CLI → azd
token = get_token(cred, "https://eventhubs.azure.net/.default")
```

Available credentials: `DefaultAzureCredential`, `ChainedTokenCredential`,
`EnvironmentCredential`, `WorkloadIdentityCredential`,
`ManagedIdentityCredential` (IMDS, App Service/Functions/Container Apps,
Cloud Shell, Azure Arc, AKS token exchange), `ClientSecretCredential`,
`ClientCertificateCredential` (PEM or PKCS#12), `ClientAssertionCredential`,
`AzureCliCredential`, `AzureDeveloperCliCredential`, plus
`get_bearer_token_provider`. `AZURE_TOKEN_CREDENTIALS=dev|prod|<CredentialName>`
narrows the default chain as in Python.

The Event Hubs and Service Bus clients also accept
`AzureNamedKeyCredential(key_name, key)`, `AzureSasCredential(signature)` or a
connection string. The Blob Storage clients accept a token credential, an
account key (`AzureNamedKeyCredential` or the key string), a SAS
(`AzureSasCredential`, a SAS string or a SAS URL) or no credential for
anonymous access.

See the [Identity API reference](@ref "Identity") for all options.
