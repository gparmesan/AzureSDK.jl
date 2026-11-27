# AMQP over WebSockets (port 443), optionally through an HTTP proxy.
#
#   EVENTHUB_NAMESPACE=myns.servicebus.windows.net EVENTHUB_NAME=myhub [HTTPS_PROXY_HOST=proxy HTTPS_PROXY_PORT=3128] \
#       julia --project examples/websockets_and_proxy.jl

using AzureSDK.EventHubs, AzureSDK.Identity

proxy = haskey(ENV, "HTTPS_PROXY_HOST") ?
    (proxy_hostname = ENV["HTTPS_PROXY_HOST"], proxy_port = parse(Int, get(ENV, "HTTPS_PROXY_PORT", "3128"))) : nothing

EventHubProducerClient(
    ENV["EVENTHUB_NAMESPACE"], ENV["EVENTHUB_NAME"], DefaultAzureCredential();
    transport_type = TransportType.AmqpOverWebsocket, http_proxy = proxy
) do producer
    send_event(producer, EventData("hello over websockets"))
    println(get_eventhub_properties(producer))
end
