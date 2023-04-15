function auth_on_register(reg)
    assert(reg.username == "test-user")
    assert(reg.password == "test-password")
    if reg.client_id == "test-change-mountpoint-pass-ovr" then
      assert(reg.mountpoint == "mpt22")
    else
      assert(reg.mountpoint == "")
    end

    assert(reg.addr == "127.0.0.1")

    if reg.client_id == "test-change-mountpoint" then
         -- we must change subscriber_id
        print("auth_on_register changed subscriber_id called")
        return {subscriber_id = {mountpoint = "override-mountpoint", client_id = "test-change-mountpoint"}}
    end

    if reg.client_id == "test-change-mountpoint-pass" then
         -- we must change subscriber_id
        print("auth_on_register changed subscriber_id called")
        return {subscriber_id = {mountpoint = "override-mountpoint", client_id = "test-change-mountpoint-pass"}}
    end

    if reg.client_id == "test-change-mountpoint-pass-ovr" then
         -- we must change subscriber_id
        print("auth_on_register changed subscriber_id called")
        return {subscriber_id = {mountpoint = "override-mountpoint", client_id = "test-change-mountpoint-pass-ovr"}}
    end

    if reg.client_id == "test-change-max-message-size-ovr" then
         -- we must change subscriber_id
        print("auth_on_register changed subscriber_id called")
        return {max_message_size = 10}
    end

    if reg.client_id == "test-change-mountpoint-pub" then
      return true
    end

    if (reg.client_id == "test-change-topic-pub") then
      return true
    end

    if reg.client_id == "test-change-qos-pub" then
      return true
    end

    assert(reg.client_id == "test-client")
    return true
end

function auth_on_publish(pub)
    assert(pub.username == "test-user")
    assert(pub.topic == "a/b/c")

    if (pub.client_id == "test-change-mountpoint-pass") then
       assert(pub.mountpoint == "override-mountpoint")
       return true
    end

    if (pub.client_id == "test-change-mountpoint-pass-ovr") then
       assert(pub.mountpoint == "override-mountpoint")
       return true
    end

    if (pub.client_id == "test-change-mountpoint-pub") then
        return {mountpoint = "override-mountpoint-pub"}
    end

    if (pub.client_id == "test-change-qos-pub") then
        assert(pub.mountpoint == "")
        return {qos = 2}
    end

    if (pub.client_id == "test-change-topic-pub") then
        assert(pub.mountpoint == "")
        return {topic = "d/e/f"}
    end

    assert(pub.mountpoint == "")
    return true
end

hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
}
