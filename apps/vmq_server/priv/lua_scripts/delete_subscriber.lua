#!lua name=delete_subscriber

--[[
ARGV[1] = mountpoint
ARGV[2] = clientId
ARGV[3] = node name
ARGV[4] = timestamp
]]

local function delete_subscriber(_KEYS, ARGV)
    local STALE_REQUEST='stale_request'
    local UNAUTHORIZED='unauthorized'

    local MP = ARGV[1]
    local clientId = ARGV[2]
    local newNode = ARGV[3]
    local timestampValue = ARGV[4]

    local subscriberKey = cmsgpack.pack({MP, clientId})
    local subscriptionField = 'subscription'
    local timestampField = 'timestamp'

    local currValues = redis.call('HMGET', subscriberKey, subscriptionField, timestampField)
    local S = currValues[1]
    local T = currValues[2]
    if S == nil or T == nil or S == false or T == false then
        return true
    elseif tonumber(timestampValue) > tonumber(T) then
        local currNode, _cs, topicsWithQoS = unpack(cmsgpack.unpack(S))
        if currNode == newNode then
            for i = 1,#topicsWithQoS,1 do
                local topic, qos = unpack(topicsWithQoS[i])
                local group, sharedTopic = string.match(topic, '^$share/(.-)/(.*)')
                if group == nil then
                    local topicKey = cmsgpack.pack({MP, topic})
                    redis.call('SREM', topicKey, cmsgpack.pack({currNode, clientId, qos}))
                else
                    local topicKey = cmsgpack.pack({MP, sharedTopic})
                    redis.call('SREM', topicKey, cmsgpack.pack({currNode, group, clientId, qos}))
                end
            end
            redis.call('DEL', subscriberKey)
            return true
        end
        return redis.error_reply(UNAUTHORIZED)
    else
        return redis.error_reply(STALE_REQUEST)
    end
end

redis.register_function('delete_subscriber', delete_subscriber)
