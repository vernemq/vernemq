#!lua name=fetch_matched_topic_subscribers

--[[
Input:
ARGV[1] = mountpoint
ARGV[2] = number of matched topics
ARGV[3] = topic1
ARGV[4] = topic2
.
.
.

Output:
Subscribers(Table) | Error
]]

local function fetch_matched_topic_subscribers(_KEYS, ARGV)
    local MP = ARGV[1]
    local numOfTopics = tonumber(ARGV[2])

    local subscribersList = {}
    local numOfSubscribers = 0

    for i = 1,numOfTopics,1 do
        local topic = ARGV[2 + i]
        local topicKey = cmsgpack.pack({MP, topic})
        local subscribers = redis.call('SMEMBERS', topicKey)
        for j = 1,#subscribers, 1 do
            numOfSubscribers = numOfSubscribers + 1
            subscribersList[numOfSubscribers] = cmsgpack.unpack(subscribers[j])
        end
    end

    return subscribersList
end

redis.register_function('fetch_matched_topic_subscribers', fetch_matched_topic_subscribers)
