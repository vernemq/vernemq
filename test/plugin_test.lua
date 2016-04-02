local function validate_client_id(client_id)
    if client_id == "allowed-subscriber-id" then
        return true  -- client we allow
    elseif client_id == "not-allowed-subscriber-id" then
        return false -- client we disallow
    else
        return       -- client we dont know
    end
end

function auth_on_register(reg)
    assert(reg.addr == "192.168.123.123")
    assert(reg.port == 12345)
    assert(reg.mountpoint == "")
    assert(reg.username == "test-user")
    assert(reg.password == "test-password")
    assert(reg.clean_session == true)
    if reg.client_id ~= "changed-subscriber-id" then
        print("auth_on_register called")
        return validate_client_id(reg.client_id)
    else
        -- we must change properties
        print("auth_on_register changed called")
        return {mountpoint = "mynewmount"}
    end
end

function auth_on_publish(pub)
    assert(pub.mountpoint == "")
    assert(pub.topic == "test/topic")
    assert(pub.qos == 1)
    assert(pub.payload == "hello world")
    assert(pub.retain == false)
    if pub.client_id ~= "changed-subscriber-id" then
        print("auth_on_publish called")
        return validate_client_id(pub.client_id)
    else
        -- we must change properties
        print("auth_on_publish changed called")
        return {topic = "hello/world"}
    end
end

function auth_on_subscribe(sub)
    assert(sub.mountpoint == "")
    assert(#sub.topics == 1)
    assert(sub.topics[1][1] == "test/topic")
    assert(sub.topics[1][2] == 1)
    if sub.client_id ~= "changed-subscriber-id" then
        print("auth_on_subscribe called")
        return validate_client_id(sub.client_id)
    else
        -- we must change properties
        print("auth_on_subscribe changed called")
        return {{"hello/world", 2}}
    end
end

function on_register(reg)
    assert(reg.addr == "192.168.123.123")
    assert(reg.port == 12345)
    assert(reg.mountpoint == "")
    assert(reg.client_id == "allowed-subscriber-id")
    assert(reg.username == "test-user")
    print("on_register called")
end

function on_publish(pub)
    assert(pub.client_id == "allowed-subscriber-id")
    assert(pub.mountpoint == "")
    assert(pub.topic == "test/topic")
    assert(pub.qos == 1)
    assert(pub.payload == "hello world")
    assert(pub.retain == false)
    print("publish called")
end

function on_subscribe(sub)
    assert(sub.client_id == "allowed-subscriber-id")
    assert(sub.mountpoint == "")
    assert(#sub.topics == 1)
    assert(sub.topics[1][1] == "test/topic")
    assert(sub.topics[1][2] == 1)
    print("on_subscribe called")
end

function on_unsubscribe(usub)
    assert(usub.mountpoint == "")
    assert(#usub.topics == 1)
    assert(usub.topics[1] == "test/topic")
    if usub.client_id ~= "changed-subscriber-id" then
        assert(usub.client_id == "allowed-subscriber-id")
        print("on_unsubscribe called")
    else
        -- we must change properties
        print("on_unsubscribe changed called")
        return {"hello/world"}
    end
end

function on_deliver(pub)
    assert(pub.client_id == "allowed-subscriber-id")
    assert(pub.mountpoint == "")
    assert(pub.topic == "test/topic")
    assert(pub.payload == "hello world")
    print("on_deliver called")
    return true
end

function on_offline_message(ev)
    assert(ev.client_id == "allowed-subscriber-id")
    assert(ev.mountpoint == "")
    print("on_offline_message called")
end

function on_client_wakeup(ev)
    assert(ev.client_id == "allowed-subscriber-id")
    assert(ev.mountpoint == "")
    print("on_client_wakeup called")
end

function on_client_offline(ev)
    assert(ev.client_id == "allowed-subscriber-id")
    assert(ev.mountpoint == "")
    print("on_client_offline called")
end

function on_client_gone(ev)
    assert(ev.client_id == "allowed-subscriber-id")
    assert(ev.mountpoint == "")
    print("on_client_gone called")
end

hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
    auth_on_subscribe = auth_on_subscribe,
    on_register = on_register,
    on_publish = on_publish,
    on_subscribe = on_subscribe,
    on_unsubscribe = on_unsubscribe,
    on_deliver = on_deliver,
    on_offline_message = on_offline_message,
    on_client_wakeup = on_client_wakeup,
    on_client_offline = on_client_offline,
    on_client_gone = on_client_gone
}
