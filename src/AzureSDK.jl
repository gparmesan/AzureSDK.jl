"""
    AzureSDK

Azure SDK for Julia. Submodules mirror the Python SDK packages:

- `AzureSDK.Identity`   — `azure.identity`
- `AzureSDK.EventHubs`  — `azure.eventhub`
- `AzureSDK.ServiceBus` — `azure.servicebus`
- `AzureSDK.AzureCore`  — `azure.core` (credentials, errors, retry options)
- `AzureSDK.AMQP`       — the AMQP 1.0 protocol engine used by the messaging clients
"""
module AzureSDK

include("core/AzureCore.jl")
include("amqp/AMQP.jl")
include("identity/Identity.jl")
include("messaging/Messaging.jl")
include("eventhubs/EventHubs.jl")
include("servicebus/ServiceBus.jl")

export AzureCore, AMQP, Identity, EventHubs, ServiceBus

end
