local function validate_client_id(client_id)
    if client_id == "allowed-subscriber-id" then
        return true  -- client we allow
    elseif client_id == "not-allowed-subscriber-id" then
        return false -- client we disallow
    else
        return       -- client we dont know
    end
end

function comparetables(t1, t2)
  if #t1 ~= #t2 then return false end
  for i=1,#t1 do
    if t1[i] ~= t2[i] then return false end
  end
  return true
end

function auth_on_register(reg)
    assert(reg.addr == "192.168.123.123")
    assert(reg.port == 12345)
    assert(reg.mountpoint == "")
    if reg.client_id == "undefined_creds" then
       assert(reg.username == nil)
       assert(reg.password == nil)
       return true
    end
    assert(reg.username == "test-user")
    assert(reg.password == "test-password")
    assert(reg.clean_session == true)
    if reg.client_id == "changed-subscriber-id" then
         -- we must change subscriber_id
        print("auth_on_register changed subscriber_id called")
        return {subscriber_id = {mountpoint = "override-mountpoint", client_id = "override-client-id"}}
    elseif reg.client_id == "changed-username" then
        -- we must change username
        print("auth_on_register changed username called")
        return {username = "override-username"}
    else
        print("auth_on_register called")
        return validate_client_id(reg.client_id)
    end
end


function auth_on_publish(pub)
    assert(pub.username == "test-user")
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
    assert(sub.username == "test-user")
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
    assert(pub.username == "test-user")
    assert(pub.client_id == "allowed-subscriber-id")
    assert(pub.mountpoint == "")
    assert(pub.topic == "test/topic")
    assert(pub.qos == 1)
    assert(pub.payload == "hello world")
    assert(pub.retain == false)
    print("publish called")
end

function on_subscribe(sub)
    assert(sub.username == "test-user")
    assert(sub.client_id == "allowed-subscriber-id")
    assert(sub.mountpoint == "")
    assert(#sub.topics == 1)
    assert(sub.topics[1][1] == "test/topic")
    assert(sub.topics[1][2] == 1)
    print("on_subscribe called")
end

function on_unsubscribe(usub)
    assert(usub.username == "test-user")
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
    assert(pub.username == "test-user")
    assert(pub.client_id == "allowed-subscriber-id")
    assert(pub.qos == 1)
    assert(pub.mountpoint == "")
    assert(pub.topic == "test/topic")
    assert(pub.payload == "hello world")
    assert(pub.retain == false)
    print("on_deliver called")
    return true
end

function on_offline_message(ev)
    assert(ev.client_id == "allowed-subscriber-id")
    assert(ev.mountpoint == "")
    assert(ev.topic == "test/topic")
    assert(ev.payload == "hello world")
    assert(ev.qos == 2)
    assert(ev.retain == false)
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

function on_session_expired(ev)
    assert(ev.client_id == "allowed-subscriber-id")
    assert(ev.mountpoint == "")
    print("on_session_expired called")
end

function auth_on_register_m5(reg)
    assert(reg.addr == "192.168.123.123")
    assert(reg.port == 12345)
    assert(reg.mountpoint == "")
    if reg.client_id == "undefined_creds" then
       assert(reg.username == nil)
       assert(reg.password == nil)
       return true
    elseif reg.client_id == "modify_props" then
       assert(reg.properties)
       properties = reg.properties
       assert(properties.p_session_expiry_interval == 5)
       assert(properties.p_receive_max == 10)
       assert(properties.p_topic_alias_max == 15)
       assert(properties.p_request_response_info == true)
       assert(properties.p_request_problem_info == true)

       assert(properties.p_user_property[1].k1 == "v1")
       assert(properties.p_user_property[2].k1 == "v2")
       assert(properties.p_user_property[3].k2 == "v2")
       print("auth_on_register_m5 modify props called")
       return {properties =
                  {p_user_property = {{k3 = "v3"}},
                   p_session_expiry_interval = 10}}
    end
    assert(reg.username == "test-user")
    assert(reg.password == "test-password")
    assert(reg.clean_start == true)
    if reg.client_id == "changed-subscriber-id" then
        -- we must change subscriber_id
        print("auth_on_register_m5 changed subscriber_id called")
        return {subscriber_id = {mountpoint = "override-mountpoint", client_id = "override-client-id"}}
    elseif reg.client_id == "changed-username" then
       -- we must change username
       print("auth_on_register_m5 changed username called")
       return {username = "override-username"} 
    else
        print("auth_on_register_m5 called")
        return validate_client_id(reg.client_id)
    end
    -- reg.properties are ignored for now.
end

function on_register_m5(reg)
   assert(reg.addr == "192.168.123.123")
   assert(reg.port == 12345)
   assert(reg.mountpoint == "")
   assert(reg.username == "test-user")
   assert(reg.client_id == "allowed-subscriber-id")
   assert(reg.properties)
   properties = reg.properties
   assert(properties.p_session_expiry_interval == 5)
   assert(properties.p_receive_max == 10)
   assert(properties.p_topic_alias_max == 15)
   assert(properties.p_request_response_info == true)
   assert(properties.p_request_problem_info == true)

   assert(properties.p_user_property[1].k1 == "v1")
   assert(properties.p_user_property[2].k1 == "v2")
   assert(properties.p_user_property[3].k2 == "v2")
   print("on_register_m5 props called")
end

function auth_on_publish_m5(pub)
    assert(pub.username == "test-user")
    assert(pub.mountpoint == "")
    assert(pub.topic == "test/topic")
    assert(pub.qos == 1)
    assert(pub.payload == "hello world")
    assert(pub.retain == false)
    if pub.client_id == "invalid_topic_mod" then
       return {topic = 5}
    elseif pub.client_id == "unknown_mod" then
       return {unknown = 5}
    elseif pub.client_id == "modify_props" then
       properties = pub.properties
       assert(properties)
       assert(properties.p_correlation_data == "correlation_data")
       assert(properties.p_response_topic == "response/topic")
       assert(properties.p_payload_format_indicator == "utf8")
       assert(properties.p_content_type == "content_type")
       assert(properties.p_user_property[1].k1 == "v1")
       assert(properties.p_user_property[2].k2 == "v2")

       print("auth_on_publish_m5 changed called")
       return {properties =
                  {p_correlation_data = "modified_correlation_data",
                   p_response_topic = "modified/response/topic",
                   p_payload_format_indicator = "undefined",
                   p_content_type = "modified_content_type",
                   p_user_property =
                      {{k1 = "v1"}, {k2 = "v2"}, {k3 = "v3"}}}}
    elseif pub.client_id ~= "changed-subscriber-id" then
       print("auth_on_publish_m5 called")
       return validate_client_id(pub.client_id)
    else
       -- change the publish topic
       print("auth_on_publish_m5 changed called")
       return {topic = "hello/world"}
    end
end

function on_publish_m5(pub)
   assert(pub.username == "test-user")
   assert(pub.mountpoint == "")
   assert(pub.topic == "test/topic")
   assert(pub.qos == 1)
   assert(pub.payload == "hello world")
   assert(pub.retain == false)
   properties = pub.properties
   assert(properties)
   assert(properties.p_correlation_data == "correlation_data")
   assert(properties.p_response_topic == "response/topic")
   assert(properties.p_payload_format_indicator == "utf8")
   assert(properties.p_content_type == "content_type")
   assert(properties.p_user_property[1].k1 == "v1")
   assert(properties.p_user_property[2].k2 == "v2")
   print("on_publish_m5 called")
end

function on_deliver_m5(pub)
   assert(pub.username == "test-user")
   assert(pub.client_id == "allowed-subscriber-id")
   assert(pub.qos == 1)
   assert(pub.mountpoint == "")
   assert(pub.topic == "test/topic")
   assert(pub.payload == "hello world")
   assert(pub.retain == false)
   assert(pub.properties)
   properties = pub.properties
   assert(properties.p_correlation_data == "correlation_data")
   assert(properties.p_response_topic == "response/topic")
   assert(properties.p_payload_format_indicator == "utf8")
   assert(properties.p_content_type == "content_type")
   assert(properties.p_user_property[1].k1 == "v1")
   assert(properties.p_user_property[2].k2 == "v2")

   print("on_deliver_m5 called")
   return {properties =
              {p_correlation_data = "modified_correlation_data",
               p_response_topic = "modified/response/topic",
               p_payload_format_indicator = "undefined",
               p_content_type = "modified_content_type",
               p_user_property =
                  {{k1 = "v1"}, {k2 = "v2"}, {k3 = "v3"}}}}
end

function auth_on_subscribe_m5(sub)
   assert(sub.username == "test-user")
   assert(sub.mountpoint == "")
   assert(#sub.topics == 1)
   assert(sub.topics[1][1] == "test/topic")
   assert(sub.topics[1][2][1] == 1)
   if sub.client_id ~= "changed-subscriber-id" then
      if sub.client_id == "allowed-subscriber-id" then
         assert(sub.properties)
         properties = sub.properties
         assert(properties.p_user_property[1].k1 == "v1")
         assert(comparetables(properties.p_subscription_id,{1, 2, 3}))
      end
      print("auth_on_subscribe_m5 called")
      return validate_client_id(sub.client_id)
   else
      -- change the subscription
      print("auth_on_subscribe changed_m5 called")
      return {topics = {{"hello/world", {2, {rap = true}}}}}
   end
end

function on_subscribe_m5(sub)
   assert(sub.username == "test-user")
   assert(sub.mountpoint == "")
   assert(#sub.topics == 1)
   assert(sub.topics[1][1] == "test/topic")
   assert(sub.topics[1][2][1] == 1)
   assert(sub.client_id, "allowed-subscriber-id")
   assert(sub.properties)
   properties = sub.properties
   assert(properties.p_user_property[1].k1 == "v1")
   assert(comparetables(properties.p_subscription_id,{1, 2, 3}))
   print("on_subscribe_m5 called")
end

function on_unsubscribe_m5(usub)
   assert(usub.username == "test-user")
   assert(usub.mountpoint == "")
   assert(#usub.topics == 1)
   assert(usub.topics[1] == "test/topic")
   assert(usub.properties)
   properties = usub.properties
   assert(properties.p_user_property[1].k1 == "v1")
   -- we must change properties
   print("on_unsubscribe_m5 changed called")
   return {topics = {"hello/world"}}
end

function on_auth_m5(auth)
   assert(auth.client_id == "allowed-subscriber-id")
   assert(auth.mountpoint == "")
   assert(auth.properties)
   properties = auth.properties
   assert(properties.p_authentication_method == "AUTH_METHOD")
   assert(properties.p_authentication_data == "AUTH_DATA0")
   return {properties =
              {p_authentication_method = "AUTH_METHOD",
               p_authentication_data = "AUTH_DATA1"}}
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
    on_client_gone = on_client_gone,
    on_session_expired = on_session_expired,

    auth_on_register_m5 = auth_on_register_m5,
    on_register_m5 = on_register_m5,
    auth_on_subscribe_m5 = auth_on_subscribe_m5,
    on_subscribe_m5 = on_subscribe_m5,
    on_unsubscribe_m5 = on_unsubscribe_m5,
    auth_on_publish_m5 = auth_on_publish_m5,
    on_publish_m5 = on_publish_m5,
    on_deliver_m5 = on_deliver_m5,
    on_auth_m5 = on_auth_m5,
}
