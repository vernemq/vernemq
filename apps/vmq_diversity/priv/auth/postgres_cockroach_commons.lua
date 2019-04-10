-- Copyright 2019 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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

-- PostgreSQL Configuration, read the documentation below to properly
-- provision your database.

-- In order to use this Lua plugin you must deploy the following database
-- schema and grant the user configured above with the required privileges:
--[[
  CREATE EXTENSION pgcrypto;
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
-- This plugin relies on a PostgreSQL version that supports the `gen_salt('bf')
-- built-in function, as the passwords are stored as blowfish hashes.
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
  	       gen_salt('bf')::text AS salt,
  	       '[{"pattern": "a/b/c"}, {"pattern": "c/b/#"}]'::json AS publish_acl,
  	       '[{"pattern": "a/b/c"}, {"pattern": "c/b/#"}]'::json AS subscribe_acl
  	)
  INSERT INTO vmq_auth_acl (mountpoint, client_id, username, password, publish_acl, subscribe_acl)
  	SELECT
  		x.mountpoint,
  		x.client_id,
  		x.username,
  		crypt(x.password, x.salt),
  		publish_acl,
  		subscribe_acl
  	FROM x;
]]--
--
-- 	The JSON array passed as publish/subscribe ACL contains the ACL objects
-- 	this particular user. MQTT wildcards as well as the variable
-- 	substitution for %m (mountpoint), %c (client_id), %u (username) are allowed
-- 	inside a pattern.
--
-- Note, it is also possible to use client-side password validation
-- using for example `bcrypt` as the hash method. Then an example
-- client ACL would look like the following:
--
--[[
  WITH x AS (
      SELECT
          ''::text AS mountpoint,
             'test-client'::text AS client_id,
             'test-user'::text AS username,
             '$2a$12$97PlnSsouvCV7HaxDPV80.EXfsKM4Fg7DAwWhSbGJ6O5CpNep20n2'::text AS hash,
             '[{"pattern": "a/b/c"}, {"pattern": "c/b/#"}]'::json AS publish_acl,
             '[{"pattern": "a/b/c"}, {"pattern": "c/b/#"}]'::json AS subscribe_acl
      )
  INSERT INTO vmq_auth_acl (mountpoint, client_id, username, password, publish_acl, subscribe_acl)
      SELECT
          x.mountpoint,
          x.client_id,
          x.username,
          x.hash,
          publish_acl,
          subscribe_acl
      FROM x;
]]--
--
-- IF YOU USE THE SCHEMA PROVIDED ABOVE NOTHING HAS TO BE CHANGED IN THE
-- FOLLOWING SCRIPT.
function auth_on_register_common(db_library, reg)
   method = db_library.hash_method()
   if reg.username ~= nil and reg.password ~= nil then
      if client_side_hashing(method) then
         -- use client side hash functions
         results = db_library.execute(
            pool,
            [[SELECT publish_acl::TEXT, subscribe_acl::TEXT, password AS passhash
              FROM vmq_auth_acl
              WHERE
                mountpoint=$1 AND
                client_id=$2 AND
                username=$3]],
            reg.mountpoint,
            reg.client_id,
            reg.username)
         if #results == 1 then
            row = results[1]
            if row.passhash == do_hash(method, reg.password, row.passhash) then
               cache_result(reg, row)
               return true
            else
              return false
           end
        else
            return false
         end
      else
         -- use server side hash functions
         if method == "crypt" then
            -- only supported in postgresql
            server_hash = "crypt($4, password)"
         elseif method == "sha256" then
            -- only supported in cockroachdb
            server_hash = "sha256($4:::TEXT)"
         else
            return false
         end
         results = db_library.execute(
            pool,
            [[SELECT publish_acl::TEXT, subscribe_acl::TEXT
              FROM vmq_auth_acl
              WHERE
                mountpoint=$1 AND
                client_id=$2 AND
                username=$3 AND
                password=]]..server_hash,
            reg.mountpoint,
            reg.client_id,
            reg.username,
            reg.password)
         if #results == 1 then
            row = results[1]
            cache_result(reg, row)
            return true
         else
            return false
         end
      end
   else
      return false
   end
end

function client_side_hashing(method)
   return method == "bcrypt"
end

function do_hash(method, password, passhash)
   if method == "bcrypt" then
      return bcrypt.hashpw(password, passhash)
   else
      return false
   end
end

function cache_result(reg, row)
   publish_acl = json.decode(row.publish_acl)
   subscribe_acl = json.decode(row.subscribe_acl)
   cache_insert(
      reg.mountpoint,
      reg.client_id,
      reg.username,
      publish_acl,
      subscribe_acl)
end
