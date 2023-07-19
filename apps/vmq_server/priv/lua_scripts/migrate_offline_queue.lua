#!lua name=migrate_offline_queue

--[[
Input:
ARGV[1] = mountpoint
ARGV[2] = clientId
ARGV[3] = old node name
ARGV[4] = new node name
ARGV[5] = timestamp

Output:
nil | Node | Error
]]

local function removeTopicsForRouting(MP, node, clientId, topicsWithQoS)
    for j = 1,#topicsWithQoS,1 do
        local topic, qos = unpack(topicsWithQoS[j])
        local group, sharedTopic = string.match(topic, '^$share/(.-)/(.*)')
        if group == nil then
            local topicKey = cmsgpack.pack({MP, topic})
            redis.call('SREM', topicKey, cmsgpack.pack({node, clientId, qos}))
        else
            local topicKey = cmsgpack.pack({MP, sharedTopic})
            redis.call('SREM', topicKey, cmsgpack.pack({node, group, clientId, qos}))
        end
    end
end

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

local function migrate_offline_queue(_KEYS, ARGV)
    local STALE_REQUEST='stale_request'
    local toboolean = { ["true"]=true, ["false"]=false }

    local MP = ARGV[1]
    local clientId = ARGV[2]
    local oldNode = ARGV[3]
    local newNode = ARGV[4]
    local timestampValue = ARGV[5]

    local subscriberKey = cmsgpack.pack({MP, clientId})
    local subscriptionField = 'subscription'
    local timestampField = 'timestamp'

    local currValues = redis.call('HMGET', subscriberKey, subscriptionField, timestampField)
    local S = currValues[1]
    local T = currValues[2]
    if S == nil or T == nil or S == false or T == false then
        return nil
    else
        local currNode, cs, topicsWithQoS = unpack(cmsgpack.unpack(S))
        if currNode == oldNode and cs == true and tonumber(timestampValue) > tonumber(T) then
            -- delete this subscriber from redis
            removeTopicsForRouting(MP, currNode, clientId, topicsWithQoS)
            redis.call('DEL', subscriberKey)
            redis.call('SREM', currNode, subscriberKey)
            return nil
        elseif currNode == oldNode and cs == false and tonumber(timestampValue) > tonumber(T) then
            -- remap subscriber
            local subscriptionValue = {newNode, false, topicsWithQoS}
            redis.call('HMSET', subscriberKey, subscriptionField, cmsgpack.pack(subscriptionValue), timestampField, timestampValue)
            updateNodeForRouting(MP, clientId, topicsWithQoS, currNode, newNode)
            redis.call('SMOVE', currNode, newNode, subscriberKey)
            return newNode
        elseif currNode ~= oldNode then
            -- subscriber reconnected to live node before migration
            return currNode
        else
            return nil
        end
    end
end

redis.register_function('migrate_offline_queue', migrate_offline_queue)
