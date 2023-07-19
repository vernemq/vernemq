#!lua name=poll_main_queue

--[[
Input:
KEYS[1] = MainQueue
ARGV[1] = maxMessagesCount

Output:
nil | {Subscriber, Msg, TimeSpent} | Error
]]

local function poll_main_queue(KEYS, ARGV)
    local mainQueue = KEYS[1]
    local maxMsgCount = tonumber(ARGV[1])

    local Res = {}
    Res = redis.call('BLMPOP', 10, 1, mainQueue,  'RIGHT', 'COUNT', maxMsgCount)
    if Res == nil or Res == false then
        return nil
    end

    local MsgTable = Res[2]
    if MsgTable == nil or MsgTable == false then
        return nil
    end

    local t = redis.call('TIME')
    local response = {}
    for i = 1,#MsgTable,1 do
        local unpackedMsg = cmsgpack.unpack(MsgTable[i])
        local timeSpentInMainQueue = ((t[1] - unpackedMsg[3])*1000000) + (t[2] - unpackedMsg[4])
        response[i] = {unpackedMsg[1], unpackedMsg[2], timeSpentInMainQueue}
    end

    return response
end

redis.register_function('poll_main_queue', poll_main_queue)
