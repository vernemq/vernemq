mountpoint = ""
client_id = "test-client"
user = "test-user"
acls = {"a/+/c", "c/#"}

-- same rules for publish and subscribe
assert(auth_cache.insert(mountpoint, client_id, user, acls, acls) == true)

assert(auth_cache.match_subscribe(mountpoint, client_id, "a/+/c") == true)
assert(auth_cache.match_subscribe(mountpoint, client_id, "a/b/c") == true)
assert(auth_cache.match_subscribe(mountpoint, client_id, "c/b/a") == true)
assert(auth_cache.match_subscribe(mountpoint, client_id, "a/b/d") == false)
assert(auth_cache.match_subscribe(mountpoint, client_id, "d/d/d") == false)

assert(auth_cache.match_publish(mountpoint, client_id, "a/b/c") == true)
assert(auth_cache.match_publish(mountpoint, client_id, "c/b/a") == true)
assert(auth_cache.match_publish(mountpoint, client_id, "a/b/d") == false)
assert(auth_cache.match_publish(mountpoint, client_id, "d/d/d") == false)

function auth_on_register(reg)
    if reg.client_id == "allowed-subscriber-id" then
        acls = {"a/%m/%u/%c/+/#", "a/b/c"}
        -- the acls above would allow to publish and subscribe to
        -- "a//test-user/allowed-subscriber-id/+/#"
        -- "a/b/c"
        assert(auth_cache.insert(
                reg.mountpoint, 
                reg.client_id, 
                reg.username, 
                acls, 
                acls) == true)
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

hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
    auth_on_subscribe = auth_on_subscribe,
    on_client_gone = on_offline,
    on_client_offline = on_offline
}
