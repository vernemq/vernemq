-- Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- MySQL Configuration, read the documentation below to properly
-- provision your database.
require "auth/auth_commons"

function cache_insert_loc(mountpoint, username, publish_acl, subscribe_acl)
    type_assert(mountpoint, "string", "mountpoint")
    type_assert(username, "string", "username")
    type_assert(publish_acl, {"table", "nil"}, "publish_acl")
    type_assert(subscribe_acl, {"table", "nil"}, "subscribe_acl")
    validate_acls_loc(publish_acl)
    validate_acls_loc(subscribe_acl)
    auth_cache.insert(mountpoint, username, username, publish_acl, subscribe_acl)
end

function validate_acls_loc(acls) 
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
            cache_insert_loc(
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
