#!lua name=get_live_nodes

--[[
Input:
ARGV[1] = nodeName

Output:
LiveNodes(Table) | Error
]]

local function get_live_nodes(_KEYS, ARGV)
    local nodeName = ARGV[1]
    local t = redis.call('TIME')
    local expiredTime = t[1] - 3

    redis.call('ZADD', 'cluster', t[1], nodeName)
    redis.call('ZREMRANGEBYSCORE', 'cluster', '-inf', expiredTime)
    return redis.call('ZRANGE', 'cluster', expiredTime, 'inf', 'BYSCORE')
end

redis.register_function('get_live_nodes', get_live_nodes)
