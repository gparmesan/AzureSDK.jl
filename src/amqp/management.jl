# Request/response over a pair of links (AMQP management pattern), used for
# claims-based security ($cbs) and the Event Hubs / Service Bus management
# nodes ($management).

mutable struct ManagementLink
    session::Session
    address::String
    reply_to::String
    sender::SenderLink
    receiver::ReceiverLink
    pending::Dict{Any,Any}          # request message-id => Ref{Union{Nothing,AMQPMessage}}
    cond::Threads.Condition
    next_id::UInt64
end

function Base.show(io::IO, m::ManagementLink)
    print(io, "ManagementLink(", m.address, ")")
end

"""
    open_management_link(session, address; timeout=60) -> ManagementLink
"""
function open_management_link(s::Session, address::AbstractString; timeout::Real=60.0,
    reply_to::AbstractString=string(address, "-client-", uuid4()))
    c = s.conn::Connection
    base = string(address, "-", string(uuid4())[1:8])
    m_ref = Ref{Any}(nothing)
    on_delivery = function (d::IncomingDelivery)
        # runs on the reader task with the connection lock held
        m = m_ref[]
        if !d.settled
            d.settled = true
            delete!(d.link.unsettled, d.delivery_id)
            _send_performative!(c, Disposition(role=ROLE_RECEIVER, first=d.delivery_id, settled=true, state=Accepted());
                channel=d.link.session.channel)
        end
        m === nothing && return
        msg = try
            decode_message(d.payload)
        catch err
            @debug "discarding undecodable management response" exception = err
            return
        end
        props = msg.properties
        key = props === nothing ? nothing : something(props.correlation_id, props.message_id, Some(nothing))
        slot = get(m.pending, key, nothing)
        if slot !== nothing
            slot[] = msg
            notify(m.cond)
        end
    end
    receiver = attach_receiver(s, base * "-receiver", Source(address=String(address));
        target=Target(address=String(reply_to)), prefetch=50, on_delivery=on_delivery, timeout=timeout,
        snd_settle_mode=SND_SETTLED)
    sender = try
        attach_sender(s, base * "-sender", Target(address=String(address));
            source=Source(address=String(reply_to)), snd_settle_mode=SND_SETTLED, timeout=timeout)
    catch
        try detach(receiver) catch end
        rethrow()
    end
    m = ManagementLink(s, String(address), String(reply_to), sender, receiver, Dict{Any,Any}(),
        Threads.Condition(c.lock), UInt64(0))
    m_ref[] = m
    return m
end

is_open(m::ManagementLink) = is_attached(m.sender) && is_attached(m.receiver)

function Base.close(m::ManagementLink)
    try detach(m.sender) catch end
    try detach(m.receiver) catch end
    return nothing
end

"""
    request(mgmt, message; timeout=60) -> AMQPMessage

Send a request and wait for the correlated response.
"""
function request(m::ManagementLink, msg::AMQPMessage; timeout::Real=60.0)
    c = m.session.conn::Connection
    deadline = deadline_after(timeout)
    slot = Ref{Union{Nothing,AMQPMessage}}(nothing)
    id = @lock c.lock begin
        m.next_id += UInt64(1)
        mid = string(m.next_id, "-", string(uuid4())[1:8])
        m.pending[mid] = slot
        mid
    end
    try
        p = msg.properties
        msg.properties = p === nothing ? Properties(message_id=id, reply_to=m.reply_to) :
                         with_fields(p; message_id=id, reply_to=m.reply_to)
        send_transfer(m.sender, encode_message(msg); settled=true, timeout=timeout)
        lock(c.lock)
        try
            ok = wait_until(() -> slot[] !== nothing || !(m.receiver.state === :attached), m.cond, deadline)
            slot[] === nothing || return slot[]
            ok || throw(timeout_error("response from $(m.address)"))
            _link_usable(m.receiver)
            throw(AMQPException(:link, COND_DETACH_FORCED, "management link closed"))
        finally
            unlock(c.lock)
        end
    finally
        @lock c.lock delete!(m.pending, id)
    end
end

"""
    response_status(msg) -> (status_code::Int, description::String, condition)

Extract the status from a management/CBS response (handles both the
`status-code` and `statusCode` spellings used by the services).
"""
function response_status(msg::AMQPMessage)
    ap = something(msg.application_properties, Dict{String,Any}())
    code = get(ap, "status-code", get(ap, "statusCode", nothing))
    desc = get(ap, "status-description", get(ap, "statusDescription", ""))
    cond = get(ap, "error-condition", get(ap, "errorCondition", nothing))
    return (code === nothing ? 0 : Int(code)), string(something(desc, "")), cond
end

"""
    put_token(cbs, audience, token; token_type="jwt", expires_on=nothing, timeout=60)

Authorize access to `audience` on this connection with claims-based security.
"""
function put_token(cbs::ManagementLink, audience::AbstractString, token::AbstractString;
    token_type::AbstractString="jwt", expires_on::Union{Nothing,Real}=nothing, timeout::Real=60.0)
    ap = Dict{String,Any}(
        "operation" => "put-token",
        "type" => String(token_type),
        "name" => String(audience),
    )
    expires_on === nothing || (ap["expiration"] = UNIX_EPOCH + Dates.Millisecond(round(Int64, expires_on * 1000)))
    msg = AMQPMessage(body_type=:value, body=String(token), application_properties=ap)
    resp = request(cbs, msg; timeout=timeout)
    status, desc, _ = response_status(resp)
    if !(status in (200, 202))
        cond = status == 401 ? COND_UNAUTHORIZED : status == 404 ? COND_NOT_FOUND :
               status == 403 ? COND_UNAUTHORIZED : Symbol("amqp:internal-error")
        throw(AMQPException(:connection, cond, "CBS put-token for $(audience) failed with status $status: $desc";
            info=Dict("status-code" => status)))
    end
    return status
end
