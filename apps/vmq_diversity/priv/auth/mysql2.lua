-- Copyright 2024- Octavo Labs/VerneMQ, Switzerland
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

-- In order to use this Lua plugin you must deploy the following database
-- schema and grant the user configured above with the required privileges:
--[[
   CREATE TABLE vmq_auth_acl
   (
     mountpoint VARCHAR(10) NOT NULL,
     client_id VARCHAR(128) NOT NULL,
     username VARCHAR(128) NOT NULL,
     password VARCHAR(128),
     publish_acl TEXT,
     subscribe_acl TEXT,
     CONSTRAINT vmq_auth_acl_primary_key PRIMARY KEY (mountpoint, client_id, username)
   )
  ]] --
-- To insert a client ACL use a similar SQL statement:
-- NOTE THAT `PASSWORD()` NEEDS TO BE SUBSTITUTED ACCORDING TO THE HASHING METHOD
-- CONFIGURED IN `vmq_diversity.mysql.password_hash_method`. CHECK THE MYSQL DOCS TO
-- FIND THE MATCHING ONE AT https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html.
--
--
--[[
--
   INSERT INTO vmq_auth_acl
   (mountpoint, client_id, username,
    password, publish_acl, subscribe_acl)
 VALUES
   ('', 'test-client', 'test-user',
    PASSWORD('123'), '[{"pattern":"a/b/c"},{"pattern":"c/b/#"}]',
                     '[{"pattern":"a/b/c"},{"pattern":"c/b/#"}]');

]]--
-- 	The JSON array passed as publish/subscribe ACL contains the topic patterns
-- 	allowed for this particular user. MQTT wildcards as well as the variable
-- 	substitution for %m (mountpoint), %c (client_id), %u (username) are allowed
-- 	inside a pattern.
--
--
-- IF YOU USE THE SCHEMA PROVIDED ABOVE NOTHING HAS TO BE CHANGED IN THE
-- FOLLOWING SCRIPT.
function validate_result_server_side(results, reg)
    if #results > 0 then
        local targetRow
        --   search for a specific rule for the client client_id
        for _, row in ipairs(results) do
            if row.client_id ~= '*' then
                targetRow = row
                break
            end
        end

        -- no specific rule, get default rule for all clients
        if targetRow == nil then
            targetRow = results[1]
        end

        local publish_acl = json.decode(targetRow.publish_acl)
        local subscribe_acl = json.decode(targetRow.subscribe_acl)
        cache_insert(reg.mountpoint, reg.client_id, reg.username, publish_acl, subscribe_acl)
        return true
    end
    return false
end

function auth_on_register(reg)
    if reg.username ~= nil and reg.password ~= nil then
        pwd = obf.decrypt(reg.password)
        results = mysql2.execute(pool,
            [[SELECT publish_acl, subscribe_acl, client_id
              FROM vmq_auth_acl
              WHERE
                mountpoint=? AND
                (client_id=? OR client_id='*') AND
                username=? AND
                password=]] .. mysql2.hash_method(), reg.mountpoint, reg.client_id, reg.username, pwd)
        return validate_result_server_side(results, reg)
    end
    return false
end

pool = "auth_mysql2"
config = {
    pool_id = pool
}

mysql2.ensure_pool(config)
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
