#!lua name=pop_offline_message

--[[
Input:
KEYS[1] = subscriberId

Output: count(Number) | Error
]]

local function pop_offline_message(KEYS, _ARGV)
    local OFFLINE_MESSAGE_COUNT_METRIC = 'offline_messages_count'

    local subscriberId = KEYS[1]

    redis.call('LPOP', subscriberId, 1)
    return redis.call('DECR', OFFLINE_MESSAGE_COUNT_METRIC)
end

redis.register_function('pop_offline_message', pop_offline_message)
