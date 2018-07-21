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

-- CockroachDB Configuration, read the documentation below to properly
-- provision your database.
require "auth/auth_commons"

-- In order to use this Lua plugin you must deploy the following database
-- schema and grant the user configured above with the required privileges:
--[[ 
  CREATE TABLE vmq_auth_acl
  (
    mountpoint character varying(10) NOT NULL,
    client_id character varying(128) NOT NULL,
    username character varying(128) NOT NULL,
    password character varying(128),
    publish_acl json,
    subscribe_acl json,
    CONSTRAINT vmq_auth_acl_primary_key PRIMARY KEY (mountpoint, client_id, username)
  );
]]--
-- Passwords are stored as SHA-256 hashes.
-- Moreover the json datatype is used for storing the publish/subscribe ACLs.
--
-- To insert a client ACL use a similar SQL statement:
--[[
  WITH x AS (
      SELECT
          ''::text AS mountpoint,
  	       'test-client'::text AS client_id,
  	       'test-user'::text AS username,
  	       '123'::text AS password,
  	       '[{"pattern": "a/b/c"}, {"pattern": "c/b/#"}]'::json AS publish_acl,
  	       '[{"pattern": "a/b/c"}, {"pattern": "c/b/#"}]'::json AS subscribe_acl
  	) 
  INSERT INTO vmq_auth_acl (mountpoint, client_id, username, password, publish_acl, subscribe_acl)
  	SELECT 
  		x.mountpoint,
  		x.client_id,
  		x.username,
  		sha256(x.password),
  		publish_acl,
  		subscribe_acl
  	FROM x;
]]--
-- 	The JSON array passed as publish/subscribe ACL contains the ACL objects
-- 	this particular user. MQTT wildcards as well as the variable 
-- 	substitution for %m (mountpoint), %c (client_id), %u (username) are allowed
-- 	inside a pattern. 
--
-- 
-- IF YOU USE THE SCHEMA PROVIDED ABOVE NOTHING HAS TO BE CHANGED IN THE
-- FOLLOWING SCRIPT.
function auth_on_register(reg)
    if reg.username ~= nil and reg.password ~= nil then
        results = postgres.execute(pool, 
            [[SELECT publish_acl, subscribe_acl 
              FROM vmq_auth_acl
              WHERE 
                mountpoint=$1 AND
                client_id=$2 AND
                username=$3 AND
                password=sha256($4:::TEXT)
            ]], 
            reg.mountpoint, 
            reg.client_id,
            reg.username,
            reg.password)
        if #results == 1 then
            row = results[1]
            publish_acl = json.decode(row.publish_acl)
            subscribe_acl = json.decode(row.subscribe_acl)
            cache_insert(
                reg.mountpoint, 
                reg.client_id, 
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

pool = "auth_crdb"
config = {
    pool_id = pool,
}

postgres.ensure_pool(config)
hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
    auth_on_subscribe = auth_on_subscribe,
    on_unsubscribe = on_unsubscribe,
    on_client_gone = on_client_gone,
    on_client_offline = on_client_offline
}