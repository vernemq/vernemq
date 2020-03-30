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

-- MongoDB Configuration, read the documentation below to properly
-- provision your database.

require "auth/auth_commons"

-- In order to use this Lua plugin your MongoDB docs used for authentication 
-- and authorization must contain the following properties:
--
--  - mountpoint: STRING
--  - client_id: STRING
--  - username: STRING
--  - passhash: STRING (bcrypt)
--  - publish_acl: [PubAclObj]  (Array of PubAclObj)
--  - subscribe_acl: [SubAclObj]  (Array of SubAclObj)
--
-- 	The BSON array passed as publish/subscribe ACL contains the ACL objects 
-- 	for this particular user. 
--
--  PubAclObj = {
--      // required
--      pattern: STRING  (use wildcards +/# and substitutions %m/%c/%u)
--
--      // optional rules
--      max_qos: INT
--      max_payload_size: INT
--      allowed_retain: BOOL
--
--      // optional modifiers, override properties of the publish
--      modifiers: {
--          topic: STRING (no wildcards ofc.)
--          payload: STRING
--          qos: INT
--          retain: BOOL
--          mountpoint: STRING
--      }
--  }
--
--  SubAclObj = {
--      // required
--      pattern: STRING  (use wildcards +/# and substitutions %m/%c/%u)
--
--      // optional rules
--      max_qos: INT
--
--      // optional modifiers, override properties of the subscribe
--      modifiers: [
--          {Topic : STRING, QoS : INT}
--      ]
--
--  }
-- 
-- IF YOU USE THE DOCUMENT SCHEMA PROVIDED ABOVE NOTHING HAS TO BE CHANGED IN THE
-- FOLLOWING SCRIPT.
function auth_on_register(reg)
    if reg.username ~= nil and reg.password ~= nil then
        doc = mongodb.find_one(pool, "vmq_acl_auth", 
                                {mountpoint = reg.mountpoint,
                                 client_id = reg.client_id,
                                 username = reg.username})
        if doc ~= false then
            if doc.passhash == bcrypt.hashpw(reg.password, doc.passhash) then
                cache_insert(
                    reg.mountpoint, 
                    reg.client_id, 
                    reg.username,
                    doc.publish_acl,
                    doc.subscribe_acl
                    )
                return true
            end
        end
    end
    return false
end

pool = "auth_mongodb"
config = {
    pool_id = pool
}

mongodb.ensure_pool(config)
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


