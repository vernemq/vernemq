function cache_insert(mountpoint, username, publish_acl, subscribe_acl)
    type_assert(mountpoint, "string", "mountpoint")
    type_assert(username, "string", "username")
    type_assert(publish_acl, {"table", "nil"}, "publish_acl")
    type_assert(subscribe_acl, {"table", "nil"}, "subscribe_acl")
    validate_acls(publish_acl)
    validate_acls(subscribe_acl)
    auth_cache.insert(mountpoint, username, username, publish_acl, subscribe_acl)
end

function validate_acls(acls) 
    if acls ~= nil then
        for i, acl in ipairs(acls) do
            for k, v in pairs(acl) do
                type_assert(k, "string", "acl key")
                if k == "modifiers" then
                    type_assert(v, "table", "acl modifiers")
                    -- TODO validate modifier types
                elseif  k == "pattern" then
                    type_assert(v, "string", "acl pattern")
                else
                    type_assert(v, {"string", "number", "boolean"}, "acl value")
                end
            end
        end
    end
end

function auth_on_register(reg)
    if reg.username ~= nil and reg.password ~= nil then
        results = mysql.execute(pool,
            [[SELECT publish_acl, subscribe_acl
              FROM vmq_auth_acl
              WHERE
                username=? AND
                password=]]..mysql.hash_method(),
            reg.username,
            reg.password)
        if #results == 1 then
            row = results[1]
            publish_acl = json.decode(row.publish_acl)
            subscribe_acl = json.decode(row.subscribe_acl)
            cache_insert(
                reg.mountpoint,
                reg.username,
                publish_acl,
                subscribe_acl
                )
            return true
        else
            return false
        end
    end
end

pool = "auth_mysql"
config = {
    pool_id = pool
}

mysql.ensure_pool(config)
hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
    auth_on_subscribe = auth_on_subscribe,
    on_unsubscribe = on_unsubscribe,
    on_client_gone = on_client_gone,
    on_client_offline = on_client_offline,
    on_session_expired = on_session_expired,

    auth_on_register_m5 = auth_on_register_m5,
    auth_on_publish_m5 = auth_on_publish_m5,
    auth_on_subscribe_m5 = auth_on_subscribe_m5,
}