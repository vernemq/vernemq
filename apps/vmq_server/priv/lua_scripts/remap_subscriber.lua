#!lua name=remap_subscriber

--[[
ARGV[1] = mountpoint
ARGV[2] = clientId
ARGV[3] = node name
ARGV[4] = clean session
ARGV[5] = timestamp
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

local function remap_subscriber(_KEYS, ARGV)
    local STALE_REQUEST='stale_request'
    local toboolean = { ["true"]=true, ["false"]=false }

    local MP = ARGV[1]
    local clientId = ARGV[2]
    local newNode = ARGV[3]
    local newCleanSession = toboolean[ARGV[4]]
    local timestampValue = ARGV[5]

    local subscriberKey = cmsgpack.pack({MP, clientId})
    local subscriptionField = 'subscription'
    local timestampField = 'timestamp'

    local currValues = redis.call('HMGET', subscriberKey, subscriptionField, timestampField)
    local S = currValues[1]
    local T = currValues[2]
    if S == nil or T == nil or S == false or T == false then
        local subscriptionValue = {newNode, newCleanSession, {}}
        redis.call('HMSET', subscriberKey, subscriptionField, cmsgpack.pack(subscriptionValue), timestampField, timestampValue)
        return {false, subscriptionValue, nil}
    elseif tonumber(timestampValue) > tonumber(T) and newCleanSession == true then
        local subscriptionValue = {newNode, true, {}}
        redis.call('HMSET', subscriberKey, subscriptionField, cmsgpack.pack(subscriptionValue), timestampField, timestampValue)
        local currNode, _cs, topicsWithQoS = unpack(cmsgpack.unpack(S))
        removeTopicsForRouting(MP, currNode, clientId, topicsWithQoS)
        if currNode ~= newNode then
            return {true, subscriptionValue, currNode}
        end
        return {true, subscriptionValue, nil}
    elseif tonumber(timestampValue) > tonumber(T) and newCleanSession == false then
        local currNode, _cs, topicsWithQoS = unpack(cmsgpack.unpack(S))
        local subscriptionValue = {newNode, false, topicsWithQoS}
        redis.call('HMSET', subscriberKey, subscriptionField, cmsgpack.pack(subscriptionValue), timestampField, timestampValue)
        if currNode ~= newNode then
            updateNodeForRouting(MP, clientId, topicsWithQoS, currNode, newNode)
            return {true, subscriptionValue, currNode}
        end
        return {true, subscriptionValue, nil}
    else
        return redis.error_reply(STALE_REQUEST)
    end
end

redis.register_function('remap_subscriber', remap_subscriber)
