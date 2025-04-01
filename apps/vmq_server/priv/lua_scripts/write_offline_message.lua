#!lua name=write_offline_message

--[[
Input:
KEYS[1] = subscriberId
ARGV[1] = message

Output: count(Number) | Error
]]

local function write_offline_message(KEYS, ARGV)
    local OFFLINE_MESSAGE_COUNT_METRIC = 'offline_messages_count'

    local subscriberId = KEYS[1]
    local message = ARGV[1]

    redis.call('RPUSH', subscriberId, message)
    return redis.call('INCR', OFFLINE_MESSAGE_COUNT_METRIC)
end

redis.register_function('write_offline_message', write_offline_message)
