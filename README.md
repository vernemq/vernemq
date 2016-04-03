# vmq_diversity - A ![VerneMQ](https://vernemq.com) Plugin Builder Toolkit [![Build Status](https://travis-ci.org/erlio/vmq_diversity.svg?branch=master)](https://travis-ci.org/erlio/vmq_diversity)

vmq_diversity enables to develop VerneMQ plugins using the ![Lua scripting language](https://www.lua.org/). However, instead of the official Lua interpreter we're using the great ![Luerl Project](https://github.com/rvirding/luerl), which is an implementation of Lua 5.2 in pure Erlang.

Moreover vmq_diversity provides simple Lua libraries to communicate with MySQL, PostgreSQL, MongoDB, and Redis within your Lua VerneMQ plugins. An additional Json encoding/decoding library as well as a generic HTTP client library provide your Lua scripts a great way to talk to external services.

vmq_diversity comes itself as a VerneMQ plugin NOT currently shipped with the VerneMQ standard release. Therefore it has to be build as a separate dependency and registered in the VerneMQ plugin system using the `vmq-admin` tool.

Building the plugin:

    $ rebar3 compile

Enabling the plugin:

    $ vmq-admin plugin enable --name=vmq_diversity --path=/Abs/Path/To/vmq_diversity/_build/default/
    
Loading a Lua script:

    $ vmq-admin script load path=/Abs/Path/To/script.lua

Reloading a Lua script e.g. after change:

    $ vmq-admin script reload path=/Abs/Path/To/script.lua

## Implementing a VerneMQ plugin

A VerneMQ plugin typically consists of one or more implemented VerneMQ hooks. We tried to keep the differences between the traditional Erlang based and Lua based plugins as small as possible. In any case please checkout out the ![Plugin Development Guide](https://vernemq.com/docs/plugindevelopment/) for more information about the different flows where you're allowed to hook in as well as the description of the different hooks.

### Your first Lua plugin

Let's start with a first very basic example that implements a basic authentification and authorization scheme.

```lua
-- the function that implements the auth_on_register/5 hook
-- the reg object contains everything required to authenticate a client
--      reg.addr: IP Address e.g. "192.168.123.123"
--      reg.port: Port e.g. 12345
--      reg.mountpoint: Mountpoint e.g. ""
--      reg.username: UserName e.g. "test-user"
--      reg.password: Password e.g. "test-password"
--      reg.client_id: ClientId e.g. "test-id"
--      reg.clean_session: CleanSession Flag true
function my_auth_on_register(reg)
    -- only allow clients connecting from this host
    if reg.addr == "192.168.10.10" then
        --only allow clients with this username 
        if reg.username == "demo-user" then
            -- only allow clients with this clientid
            if reg.client_id == "demo-id" then
                return true
            end
        end
    end
    return false
end

-- the function that implements the auth_on_publish/6 hook
-- the pub object contains everything required to authorize a publish request
--      pub.mountpoint: Mountpoint e.g. ""
--      pub.client_id: ClientId e.g. "test-id"
--      pub.topic: Publish Topic e.g. "test/topic"
--      pub.qos: Publish QoS e.g. 1
--      pub.payload: Payload e.g. "hello world"
--      pub.retain: Retain flag e.g. false
function my_auth_on_publish(pub)
    -- only allow publishes on this topic with QoS = 0
    if pub.topic == "demo/topic" and pub.qos == 0 then
        return true
    end
    return false
end

-- the function that implements the auth_on_subscribe/3 hook
-- the sub object contains everything required to authorize a subscribe request
--      sub.mountpoint: Mountpoint e.g. ""
--      sub.client_id: ClientId e.g. "test-id"
--      sub.topics: A list of Topic/QoS Pairs e.g. { {"topic/1", 0}, {"topic/2, 1} }
function my_auth_on_subscribe(sub)
    local topic = sub.topics[1]
    if topic then
        -- only allow subscriptions for the topic "demo/topic" with QoS = 0
        if topic[1] == "demo/topic" and topic[2] == 0 then
            return true
        end
    end
    return false
end

-- the hooks table specifies which hooks this plugin is implementing
hooks = {
    auth_on_register = my_auth_on_register,
    auth_on_publish = my_auth_on_publish,
    auth_on_subscribe = my_auth_on_subscribe
}
```

## Accessing the Data Providers

TODO: (see the tests/[mysql|postgres|mongodb|redis].lua)

## Accessing the HTTP and Json Client Library

TODO: (see the tests/[http|json].lua)

## Accessing the Logger

TODO: (see the tests/logger.lua)



