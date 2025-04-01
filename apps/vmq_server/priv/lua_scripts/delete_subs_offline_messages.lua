#!lua name=delete_subs_offline_messages

--[[
Input:
KEYS[1] = subscriberId

Output: count(Number) | Error
]]

local function delete_subs_offline_messages(KEYS, _ARGV)
    local OFFLINE_MESSAGE_COUNT_METRIC = 'offline_messages_count'

    local subscriberId = KEYS[1]

    local size = redis.call('LLEN', subscriberId)
    redis.call('DEL', subscriberId)
    return redis.call('DECRBY', OFFLINE_MESSAGE_COUNT_METRIC, size)
end

redis.register_function('delete_subs_offline_messages', delete_subs_offline_messages)
