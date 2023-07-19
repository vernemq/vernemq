#!lua name=subscribe

--[[
Input:
ARGV[1] = mountpoint
ARGV[2] = clientId
ARGV[3] = node name
ARGV[4] = timestamp
ARGV[5] = number of newTopicsWithQoS
ARGV[6] = topic1
ARGV[7] = qos
ARGV[8] = topic2
ARGV[9] = qos
.
.
.

Output:
{} | {Node, CS, {{Topic, QoS}, ...}} | 'stale_request' | 'unauthorized' | Error
]]

local function addTopicForRouting(topic, MP, node, clientId, qos)
    local group, sharedTopic = string.match(topic, '^$share/(.-)/(.*)')
    if group == nil then
        local topicKey = cmsgpack.pack({MP, topic})
        redis.call('SADD', topicKey, cmsgpack.pack({node, clientId, qos}))
    else
        local topicKey = cmsgpack.pack({MP, sharedTopic})
        redis.call('SADD', topicKey, cmsgpack.pack({node, group, clientId, qos}))
    end
end

local function updateQoSForRouting(topic, MP, node, clientId, currQos, newQos)
    local group, sharedTopic = string.match(topic, '^$share/(.-)/(.*)')
    if group == nil then
        local topicKey = cmsgpack.pack({MP, topic})
        redis.call('SREM', topicKey, cmsgpack.pack({node, clientId, currQos}))
        redis.call('SADD', topicKey, cmsgpack.pack({node, clientId, newQos}))
    else
        local topicKey = cmsgpack.pack({MP, sharedTopic})
        redis.call('SREM', topicKey, cmsgpack.pack({node, group, clientId, currQos}))
        redis.call('SADD', topicKey, cmsgpack.pack({node, group, clientId, newQos}))
    end
end


local function mergeTopics(MP, clientId, node, numOfNewTopicsWithQoS, ARGV, existingTopicsWithQoS)
    -- ExistingTopicsWithQoS and newTopicsWithQoS in ARGV are sorted alphabetically on the basis of topic
    local topicsWithQoS = {}
    local i, j, k = 1, 1, 1
    while (i <= #existingTopicsWithQoS) and (j <= numOfNewTopicsWithQoS) do
        local newTopic = ARGV[5 + (2*j) - 1]
        local qos = ARGV[5 + (2*j)]
        if existingTopicsWithQoS[i][1] < newTopic then
            topicsWithQoS[k] = existingTopicsWithQoS[i]
            i = i + 1
        elseif newTopic < existingTopicsWithQoS[i][1] then
            topicsWithQoS[k] = {newTopic, qos}
            j = j + 1
            addTopicForRouting(newTopic, MP, node, clientId, qos)
        else
            topicsWithQoS[k] = {newTopic, qos}
            updateQoSForRouting(newTopic, MP, node, clientId, existingTopicsWithQoS[i][2], qos)
            j = j + 1
            i = i + 1
        end
        k = k + 1
    end
    while (i <= #existingTopicsWithQoS) do
        topicsWithQoS[k] = existingTopicsWithQoS[i]
        k = k + 1
        i = i + 1
    end
    while (j <= numOfNewTopicsWithQoS) do
        local newTopic = ARGV[5 + (2*j) - 1]
        local qos = ARGV[5 + (2*j)]
        topicsWithQoS[k] = {newTopic, qos}
        k = k + 1
        j = j + 1
        addTopicForRouting(newTopic, MP, node, clientId, qos)
    end
    return topicsWithQoS
end

local function subscribe(_KEYS, ARGV)
    local STALE_REQUEST='stale_request'
    local UNAUTHORIZED='unauthorized'

    local MP = ARGV[1]
    local clientId = ARGV[2]
    local newNode = ARGV[3]
    local timestampValue = ARGV[4]
    local numOfNewTopicsWithQoS = tonumber(ARGV[5])

    local subscriberKey = cmsgpack.pack({MP, clientId})
    local subscriptionField = 'subscription'
    local timestampField = 'timestamp'

    local currValues = redis.call('HMGET', subscriberKey, subscriptionField, timestampField)
    local S = currValues[1]
    local T = currValues[2]
    if S == nil or T == nil or S == false or T == false then
        local topicsWithQoS = mergeTopics(MP, clientId, newNode, numOfNewTopicsWithQoS, ARGV, {})
        local subscriptionValue = {newNode, true, topicsWithQoS}
        redis.call('HMSET', subscriberKey, subscriptionField, cmsgpack.pack(subscriptionValue), timestampField, timestampValue)
        redis.call('SADD', newNode, subscriberKey)
        return {}
    elseif tonumber(timestampValue) > tonumber(T) then
        local currSub = cmsgpack.unpack(S)
        local currNode, cs, existingTopicsWithQoS = unpack(currSub)
        if newNode == currNode then
            local newTopicsWithQoS = mergeTopics(MP, clientId, newNode, numOfNewTopicsWithQoS, ARGV, existingTopicsWithQoS)
            local subscriptionValue = {currNode, cs, newTopicsWithQoS}
            redis.call('HMSET', subscriberKey, subscriptionField, cmsgpack.pack(subscriptionValue), timestampField, timestampValue)
            return currSub
        end
        return redis.error_reply(UNAUTHORIZED)
    else
        return redis.error_reply(STALE_REQUEST)
    end
end

redis.register_function('subscribe', subscribe)
