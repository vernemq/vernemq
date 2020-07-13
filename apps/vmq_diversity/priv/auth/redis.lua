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

-- Redis Configuration, read the documentation below to properly
-- provision your database.
require "auth/auth_commons"

-- In order to use this Lua plugin you must store a JSON Object containing 
-- the following properties as Redis Value:
--
--  - passhash: STRING (bcrypt)
--  - publish_acl: [ACL]  (Array of ACL JSON Objects)
--  - subscribe_acl: [ACL]  (Array of ACL JSON Objects)
--
-- 	The JSON array passed as publish/subscribe ACL contains the ACL objects topic
-- 	for this particular user. MQTT wildcards as well as the variable 
-- 	substitution for %m (mountpoint), %c (client_id), %u (username) are allowed
-- 	inside a pattern. 
--
-- The Redis Key is the JSON Array [mountpoint, client_id, username]
-- 
-- IF YOU USE THE KEY/VALUE SCHEMA PROVIDED ABOVE NOTHING HAS TO BE CHANGED 
-- IN THE FOLLOWING SCRIPT.
function auth_on_register(reg)
    if reg.username ~= nil and reg.password ~= nil then
        key = json.encode({reg.mountpoint, reg.client_id, reg.username})
        res = redis.cmd(pool, "get " .. key)
        if res then
            res = json.decode(res)
            if res.passhash == bcrypt.hashpw(reg.password, res.passhash) then
                cache_insert(
                    reg.mountpoint, 
                    reg.client_id, 
                    reg.username,
                    res.publish_acl,
                    res.subscribe_acl
                    )
                return true
            end
        end
    end
    return false
end

pool = "auth_redis"
config = {
    pool_id = pool
}

redis.ensure_pool(config)
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
