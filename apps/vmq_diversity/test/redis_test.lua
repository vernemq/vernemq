redis.ensure_pool({pool_id = "redis_test"})

function equals(o1, o2)
    if o1 == o2 then return true end
    local o1Type = type(o1)
    local o2Type = type(o2)
    if o1Type ~= o2Type then return false end
    if o1Type ~= 'table' then return false end

    local keySet = {}

    for key1, value1 in pairs(o1) do
        local value2 = o2[key1]
        if value2 == nil or equals(value1, value2) == false then
            return false
        end
        keySet[key1] = true
    end

    for key2, _ in pairs(o2) do
        if not keySet[key2] then return false end
    end
    return true
end

function cmd(cmd, expected_result)
    result = redis.cmd("redis_test", cmd)
    assert(equals(result, expected_result), result)
end

function cmdm(cmd, args, expected_result)
    result = redis.cmd("redis_test", cmd, args)
    assert(equals(result, expected_result), result)
end

redis.cmd("redis_test", "flushall")

cmd("set mykey somevalue", true)
cmd("get mykey", "somevalue")

cmd("set mykey newval nx", nil)
cmd("set mykey newval xx", true)
cmd("get mykey", "newval")

cmd("set counter 100", true)
cmd("incr counter", "101")
cmd("incr counter", "102")
cmd("incrby counter 50", "152")

cmd("mset a 10 b 20 c 30", true)
cmd("mget a b c", {"10", "20", "30"})

cmd("set mykey x", true)
cmd("type mykey", "string")

cmd("del mykey", "1")
cmd("type mykey", "none")

cmd("set key some-value", true)
cmd("expire key 5", "1")
cmd("get key", "some-value")
-- we can't sleep here.. but the key will be expired in 5 seconds

cmd("ttl key", "5")
cmd("set key 100 ex 10", true)
cmd("ttl key", "10")

cmd("rpush mylist A", "1")
cmd("rpush mylist B", "2")
cmd("lpush mylist first", "3")
cmd("lrange mylist 0 -1", {"first", "A", "B"})

cmdm("rpush mylist 1 2 3 4 5", "foo bar", "9")
cmd("lrange mylist 0 -1", {"first", "A", "B", "1", "2", "3", "4", "5", "foo bar"})
cmd("del mylist", "1")

cmd("rpush mylist a b c", "3")
cmd("rpop mylist", "c")
cmd("rpop mylist", "b")
cmd("rpop mylist", "a")
cmd("rpop mylist", nil)

cmd("rpush mylist 1 2 3 4 5", "5")
cmd("ltrim mylist 0 2", true)
cmd("lrange mylist 0 -1", {"1", "2", "3"})

redis.cmd("redis_test", "flushall")
