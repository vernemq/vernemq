
function cache_insert(mountpoint, client_id, username, publish_acl, subscribe_acl)
    type_assert(mountpoint, "string", "mountpoint")
    type_assert(client_id, "string", "client_id")
    type_assert(username, "string", "username")
    type_assert(publish_acl, {"table", "nil"}, "publish_acl")
    type_assert(subscribe_acl, {"table", "nil"}, "subscribe_acl")
    validate_acls(publish_acl)
    validate_acls(subscribe_acl)
    auth_cache.insert(mountpoint, client_id, username, publish_acl, subscribe_acl)
end

function type_assert(v, expected, descr)
    type_v = type(v)
    if type(expected) == "table" then
        descr_ext = ""
        for i, t in ipairs(expected) do
            descr_ext = descr_ext .. t .. " "
            if type_v == t then
                return
            end
        end
        assert(false, descr .. "expects one of ( " .. descr_ext .. "), but was a " .. type_v)
    else
        assert(type_v == expected, descr .. "expects a " .. expected .. ", but was a " .. type_v)
    end
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

