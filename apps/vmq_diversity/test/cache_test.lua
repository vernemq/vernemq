mountpoint = ""
client_id = "test-client"
user = "test-user"
acls = {{pattern = "a/+/c"}, 
        {pattern = "c/#"}}

-- same rules for publish and subscribe
assert(auth_cache.insert(mountpoint, client_id, user, acls, acls) == true)

assert(auth_cache.match_subscribe(mountpoint, client_id, "a/+/c", 1) == true)
assert(auth_cache.match_subscribe(mountpoint, client_id, "a/b/c", 1) == true)
assert(auth_cache.match_subscribe(mountpoint, client_id, "c/b/a", 1) == true)
assert(auth_cache.match_subscribe(mountpoint, client_id, "a/b/d", 1) == false)
assert(auth_cache.match_subscribe(mountpoint, client_id, "d/d/d", 1) == false)

assert(auth_cache.match_publish(mountpoint, client_id, "a/b/c", 1, "hello", false) == true)
assert(auth_cache.match_publish(mountpoint, client_id, "c/b/a", 1, "hello", false) == true)
assert(auth_cache.match_publish(mountpoint, client_id, "a/b/d", 1, "hello", false) == false)
assert(auth_cache.match_publish(mountpoint, client_id, "d/d/d", 1, "hello", false) == false)

-- complex ACLs
complex_client_id = "test-client-complex"
complex_acls = {
    {
        pattern = "a/+/c",
        max_qos = 1,
        max_payload_size = 5,
        allowed_retain = false
    }
}
assert(auth_cache.insert(mountpoint, complex_client_id, user, complex_acls, complex_acls) == true)
assert(auth_cache.match_subscribe(mountpoint, complex_client_id, "a/+/c", 2) == false) -- not allowed due to max_qos 
assert(auth_cache.match_subscribe(mountpoint, complex_client_id, "a/+/c", 1) == true) -- allowed

assert(auth_cache.match_publish(mountpoint, complex_client_id, "a/b/c", 2, "hello", false) == false) -- not allowed due to max_qos
assert(auth_cache.match_publish(mountpoint, complex_client_id, "a/b/c", 1, "hello world", false) == false) -- not allowed due to max_payload_size
assert(auth_cache.match_publish(mountpoint, complex_client_id, "a/b/c", 1, "hello", true) == false) -- not allowed due to not allowed_retain
assert(auth_cache.match_publish(mountpoint, complex_client_id, "a/b/c", 1, "hello", false) == true) -- allowed


-- complex ACLs including modifiers
modifiers_client_id = "test-client-modifiers"
pub_modifiers_acls = {
    {
        pattern = "a/+/c",
        modifiers = {
            topic = "hello/world",
            payload = "hello world",
            qos = 1,
            retain = true,
            mountpoint = "override-mountpoint2"
        }
    }
}
sub_modifiers_acls = {
    {
        pattern = "a/+/c",
        modifiers = {{"hello/world", 2}}
    }
}

assert(auth_cache.insert(mountpoint, modifiers_client_id, user, pub_modifiers_acls, sub_modifiers_acls) == true)

ret = auth_cache.match_subscribe(mountpoint, modifiers_client_id, "a/+/c", 1)
assert(ret[1][1] == "hello/world")
assert(ret[1][2] == 2)

ret = auth_cache.match_publish(mountpoint, modifiers_client_id, "a/b/c", 2, "hello", false)
assert(ret.topic == "hello/world")
assert(ret.payload == "hello world")
assert(ret.qos == 1)
assert(ret.retain == true)
assert(ret.mountpoint == "override-mountpoint2")

function auth_on_register(reg)
    if reg.client_id == "allowed-subscriber-id" then
       publish_acls = {
          {
             pattern = "a/b/c"
          },
          {
             pattern = "a/%m/%u/%c/+/#",
          },
          {
             pattern = "modifiers",
             modifiers = {
                topic = "hello/world",
                payload = "hello world",
                qos = 1,
                retain = true,
                mountpoint = "override-mountpoint2"
             }

          }
       }
       subscribe_acls = {
          {
             pattern = "a/b/c",
          },
          {
             pattern = "a/%m/%u/%c/+/#",
          },
          {
             pattern = "modifiers",
             modifiers =
                {
                   {"hello/world", 2}
                },
          }
       }
       -- the acls above would allow to publish and subscribe to
       -- "a//test-user/allowed-subscriber-id/+/#"
       -- "a/b/c"
       assert(auth_cache.insert(
                 reg.mountpoint,
                 reg.client_id,
                 reg.username,
                 publish_acls,
                 subscribe_acls) == true)
       print("cache auth_on_register")
       return true
    end
    print("uncached auth_on_register")
    return true
end

function auth_on_publish(pub)
    print("uncached auth_on_publish")
    return true
end

function auth_on_subscribe(sub)
    print("uncached auth_on_subscribe")
    return true
end

function on_offline(c)
    print("clear cache, client is offline")
end

function auth_on_register_m5(reg)
   return auth_on_register(reg)
end

function auth_on_publish_m5(pub)
   return false
end

function auth_on_subscribe_m5(sub)
   return false
end

hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
    auth_on_subscribe = auth_on_subscribe,
    on_client_gone = on_offline,
    on_client_offline = on_offline,

    auth_on_publish_m5 = auth_on_publish_m5,
    auth_on_publish_m5 = auth_on_publish_m5,
    auth_on_subscribe_m5 = auth_on_subscribe_m5,

}
