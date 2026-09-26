# Ordered processing with sessions and session state.
#
#   SERVICEBUS_NAMESPACE=... SERVICEBUS_SESSION_QUEUE=... julia --project examples/servicebus_sessions.jl

using AzureSDK.ServiceBus, AzureSDK.Identity

queue = get(ENV, "SERVICEBUS_SESSION_QUEUE", "orders-sessions")
ServiceBusClient(ENV["SERVICEBUS_NAMESPACE"], DefaultAzureCredential()) do client
    get_queue_sender(client, queue) do sender
        for customer in ("alice", "bob"), step in 1:3
            send_messages(sender, ServiceBusMessage("$customer step $step"; session_id=customer))
        end
    end
    renewer = AutoLockRenewer(max_lock_renewal_duration=300)
    for _ in 1:2
        get_queue_receiver(client, queue; session_id=NEXT_AVAILABLE_SESSION, max_wait_time=5,
            auto_lock_renewer=renewer) do receiver
            session = receiver.session
            for msg in receiver
                println(session.session_id, ": ", body_as_str(msg))
                set_state(session, body_as_str(msg))
                complete_message(receiver, msg)
            end
            println("session ", session.session_id, " state: ", String(something(get_state(session), UInt8[])))
        end
    end
    close(renewer)
end
