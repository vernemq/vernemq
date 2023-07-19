#!lua name=fetch_subscriber

--[[
Input:
ARGV[1] = mountpoint
ARGV[2] = clientId

Output:
{} | {Node, CS, {{Topics,QoS}, ...}} | Error
]]

local function fetch_subscriber(_KEYS, ARGV)
    local MP = ARGV[1]
    local clientId = ARGV[2]

    local subscriberKey = cmsgpack.pack({MP, clientId})
    local subscriptionField = 'subscription'

    local subscription = redis.call('HGET', subscriberKey, subscriptionField)
    if subscription == nil or subscription == false then
        return {}
    else
        return cmsgpack.unpack(subscription)
    end
end

redis.register_function('fetch_subscriber', fetch_subscriber)
