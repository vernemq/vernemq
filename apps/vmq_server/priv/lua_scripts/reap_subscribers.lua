#!lua name=reap_subscribers

--[[
Input:
ARGV[1] = deadNode
ARGV[2] = newNode
ARGV[3] = maxClients

Output:
nil | {SubscriberId, ...} | Error
]]

local function updateNodeForRouting(MP, clientId, topicsWithQoS, currNode, newNode)
    for j = 1,#topicsWithQoS,1 do
        local topic, qos = unpack(topicsWithQoS[j])
        local group, sharedTopic = string.match(topic, '^$share/(.-)/(.*)')
        if group == nil then
            local topicKey = cmsgpack.pack({MP, topic})
            redis.call('SREM', topicKey, cmsgpack.pack({currNode, clientId, qos}))
            redis.call('SADD', topicKey, cmsgpack.pack({newNode, clientId, qos}))
        else
            local topicKey = cmsgpack.pack({MP, sharedTopic})
            redis.call('SREM', topicKey, cmsgpack.pack({currNode, group, clientId, qos}))
            redis.call('SADD', topicKey, cmsgpack.pack({newNode, group, clientId, qos}))
        end
    end
end

local function reap_subscribers(_KEYS, ARGV)
    local deadNode = ARGV[1]
    local newNode = ARGV[2]
    local maxClients = tonumber(ARGV[3])

    local subscriptionField = 'subscription'

    local subscriberKeys = redis.call('SRANDMEMBER', deadNode, maxClients)

    if next(subscriberKeys) == nil then
        return nil
    end

    local migratedSubscribers = {}
    local i = 1

    for j = 1,#subscriberKeys,1 do
        local MP, clientId = unpack(cmsgpack.unpack(subscriberKeys[j]))
        local currNode, cleanSession, topicsWithQoS = unpack(cmsgpack.unpack(redis.call('HGET', subscriberKeys[j], subscriptionField)))
        if cleanSession == true then
            for i = 1,#topicsWithQoS,1 do
                local topic, qos = unpack(topicsWithQoS[i])
                local group, sharedTopic = string.match(topic, '^$share/(.-)/(.*)')
                if group == nil then
                    local topicKey = cmsgpack.pack({MP, topic})
                    redis.call('SREM', topicKey, cmsgpack.pack({deadNode, clientId, qos}))
                else
                    local topicKey = cmsgpack.pack({MP, sharedTopic})
                    redis.call('SREM', topicKey, cmsgpack.pack({deadNode, group, clientId, qos}))
                end
            end
            redis.call('SREM', deadNode, subscriberKeys[j])
            redis.call('DEL', subscriberKeys[j])
        else
            updateNodeForRouting(MP, clientId, topicsWithQoS, deadNode, newNode)
            redis.call('SMOVE', deadNode, newNode, subscriberKeys[j])
            local subscriptionValue = {newNode, false, topicsWithQoS}
            redis.call('HSET', subscriberKeys[j], subscriptionField, cmsgpack.pack(subscriptionValue))
            migratedSubscribers[i] = {MP, clientId}
            i = i + 1
        end
    end
    return migratedSubscribers
end

redis.register_function('reap_subscribers', reap_subscribers)
