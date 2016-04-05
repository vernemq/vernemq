# vmq_diversity - A ![VerneMQ](https://vernemq.com) Plugin Builder Toolkit [![Build Status](https://travis-ci.org/erlio/vmq_diversity.svg?branch=master)](https://travis-ci.org/erlio/vmq_diversity)

vmq_diversity enables to develop VerneMQ plugins using the ![Lua scripting language](https://www.lua.org). However, instead of the official Lua interpreter we're using the great ![Luerl Project](https://github.com/rvirding/luerl), which is an implementation of Lua 5.2 in pure Erlang.

Moreover vmq_diversity provides simple Lua libraries to communicate with MySQL, PostgreSQL, MongoDB, and Redis within your Lua VerneMQ plugins. An additional Json encoding/decoding library as well as a generic HTTP client library provide your Lua scripts a great way to talk to external services.

vmq_diversity itself is a VerneMQ plugin but is currently NOT shipped with the VerneMQ standard release. Therefore it has to be build as a separate dependency and registered in the VerneMQ plugin system using the `vmq-admin` tool.

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

This subsection describes the data providers currently available to a lua script.
Every data provider is backed by a connection pool that has to be configured by
your script.

### MySQL

#### ensure_pool

```lua
mysql.ensure_pool(config)
```

Ensures that the connection pool named ```config.pool_id``` is setup in the system.
The ```config``` argument is a Lua table hodling the following keys:

- ```pool_id```: Name of the connection pool (mandatory).
- ```size```: Size of the connection pool (default is 5).
- ```user```: MySQL account name for login
- ```password```: MySQL account password for login (in clear text).
- ```host```: Host name for the MySQL server (default is localhost)
- ```port```: Port that the MySQL server is listening on (default is 3306).
- ```database```: MySQL database name.
- ```encoding```: Encoding (default is latin1)

This call throws a badarg error in case it cannot setup the pool otherwise it
returns ```true```.

#### execute

```lua
mysql.execute(pool_id, stmt, args...)
```

Executes the provided SQL statement using a connection from the connection pool.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```stmt```: A valid MySQL statement.
- ```args...```: A variable number of arguments can be passed to substitute statement parameters.

Depending on the statement this call returns ```true``` or ```false``` or a Lua array containing the resulting rows (as Lua tables). In case the statement cannot be executed a badarg error is thrown.


### PostgreSQL

#### ensure_pool

```lua
postgres.ensure_pool(config)
```

Ensures that the connection pool named ```config.pool_id``` is setup in the system.
The ```config``` argument is a Lua table hodling the following keys:

- ```pool_id```: Name of the connection pool (mandatory).
- ```size```: Size of the connection pool (default is 5).
- ```user```: Postgres account name for login
- ```password```: Postgres account password for login (in clear text).
- ```host```: Host name for the Postgres server (default is localhost)
- ```port```: Port that the Postgres server is listening on (default is 5432).
- ```database```: Postgres database name.

This call throws a badarg error in case it cannot setup the pool otherwise it
returns ```true```.

#### execute

```lua
postgres.execute(pool_id, stmt, args...)
```

Executes the provided SQL statement using a connection from the connection pool.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```stmt```: A valid MySQL statement.
- ```args...```: A variable number of arguments can be passed to substitute statement parameters.

Depending on the statement this call returns ```true``` or ```false``` or a Lua array containing the resulting rows (as Lua tables). In case the statement cannot be executed a badarg error is thrown.

### MongoDB

#### ensure_pool

```lua
mongodb.ensure_pool(config)
```

Ensures that the connection pool named ```config.pool_id``` is setup in the system.
The ```config``` argument is a Lua table hodling the following keys:

- ```pool_id```: Name of the connection pool (mandatory).
- ```size```: Size of the connection pool (default is 5).
- ```login```: MongoDB login name
- ```password```: MongoDB password for login.
- ```host```: Host name for the MongoDB server (default is localhost)
- ```port```: Port that the MongoDB server is listening on (default is 27017).
- ```database```: MongoDB database name.
- ```w_mode```: Set mode for writes either to "unsafe" or "safe".
- ```r_mode```: Set mode for reads either to "master" or "slave_ok".

This call throws a badarg error in case it cannot setup the pool otherwise it
returns ```true```.

#### insert

```lua
mongodb.insert(pool_id, collection, doc_or_docs)
```

Insert the provided document (or list of documents) into the collection.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```collection```: Name of a MongoDB collection.
- ```doc_or_docs```: A single Lua table or a Lua array containing multiple Lua tables.

The provided document can set the document id using the ```_id``` key. If the id
isn't provided one gets autogenerated. The call returns the inserted document(s) or 
throws a badarg error if it cannot insert the document(s).

#### update

```lua
mongodb.update(pool_id, collection, selector, doc)
```

Updates all documents in the collection that match the given selector.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```collection```: Name of a MongoDB collection.
- ```selector```: A single Lua table containing the filter properties.
- ```doc```: A single Lua table the update properties.

The call returns ```true``` or throws a badarg error if it cannot update the document(s).

#### delete

```lua
mongodb.delete(pool_id, collection, selector)
```

Deletes all documents in the collection that match the given selector.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```collection```: Name of a MongoDB collection.
- ```selector```: A single Lua table containing the filter properties.

The call returns ```true``` or throws a badarg error if it cannot delete the document(s).

#### find

```lua
mongodb.find(pool_id, collection, selector, args)
```

Finds all documents in the collection that match the given selector.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```collection```: Name of a MongoDB collection.
- ```selector```: A single Lua table containing the filter properties.
- ```args```: A Lua table that currently supports an optional ```projector=LuaTable``` element.

The call returns a MongoDB cursor or throws a badarg error if it cannot setup the iterator.

#### next

```lua
mongodb.next(cursor)
```

Fetches next available document given a cursor object obtained via ```find```.

The call returns the next available document or ```false``` if all documents have been fetched.

#### take

```lua
mongodb.take(cursor, nr_of_docs)
```

Fetches the next ```nr_of_docs``` documents given a cursor object obtained via ```find```.

The call returns a Lua array containing the documents or ```false``` if all documents have been fetched.

#### close

```lua
mongodb.close(cursor)
```

Closes an cleans up a cursor object obtained via ```find```.

The call returns ```true```.

#### find_one

```lua
mongodb.find_one(pool_id, collection, selector, args)
```

Finds the first document in the collection that matches the given selector.

- ```pool_id```: Name of the connection pool to use for this statement.
- ```collection```: Name of a MongoDB collection.
- ```selector```: A single Lua table containing the filter properties.
- ```args```: A Lua table that currently supports an optional ```projector=LuaTable``` element.

The call returns the matched document or ```false``` in case no document was found.

### Redis

#### ensure_pool

```lua
redis.ensure_pool(config)
```

Ensures that the connection pool named ```config.pool_id``` is setup in the system.
The ```config``` argument is a Lua table hodling the following keys:

- ```pool_id```: Name of the connection pool (mandatory).
- ```size```: Size of the connection pool (default is 5).
- ```password```: Redis password for login.
- ```host```: Host name for the Redis server (default is localhost)
- ```port```: Port that the Redis server is listening on (default is 6379).
- ```database```: Redis database (default is 0).

This call throws a badarg error in case it cannot setup the pool otherwise it
returns ```true```.

#### cmd

```lua
redis.cmd(pool_id, command, args...)
```

Executes the given Redis command.

- ```pool_id```: Name of the connection pool
- ```command```: Redis command string.
- ```args...```: Extra args.

This call returns a Lua table, ```true```, ```false```, or ```nil```. In case it cannot 
parse the command a badarg error is thrown. 


## Accessing the HTTP and Json Client Library

### HTTP Client

#### ensure_pool

```lua
http.ensure_pool(config)
```

Ensures that the connection pool named ```config.pool_id``` is setup in the system.
The ```config``` argument is a Lua table hodling the following keys:

- ```pool_id```: Name of the connection pool (mandatory).
- ```size```: Size of the connection pool (default is 10).

This call throws a badarg error in case it cannot setup the pool otherwise it
returns ```true```.

#### get, put, post, delete

```lua
http.get(pool_id, url, body, headers)
http.put(pool_id, url, body, headers)
http.post(pool_id, url, body, headers)
http.delete(pool_id, url, body, headers)
```

Executes a HTTP request with the given url and args.

- ```url```: A valid http url.
- ```body```: optional body to be included in the request.
- ```headers```: optional Lua table containing extra headers to be included in the request.

This call returns ```false``` in case of an error or a Lua table of the form:

```lua
response = {
    status = HTTP_STATUS_CODE,
    headers = Lua Table containing response headers,
    ref = Client Ref
}
```

#### body

```lua
http.body(client_ref)
```

Fetches the response body given a client ref obtained via the response Lua table.

This call returns ```false``` in case of an error or the response body.

### JSON

#### encode

```lua
json.encode(val)
```

Encodes a Lua value to a JSON string.

This call returns false if it cannot encode the given value. 

#### decode

```lua
json.decode(json_string)
```

Decodes a JSON string to a Lua value.

This call returns false if it cannot decode the JSON string.

## Accessing the Logger

```lua
log.info(log_string)
log.error(log_string)
log.warning(log_string)
log.debug(log_string)
```

Uses the VerneMQ logging infrastructure to log the given ```log_string```.

