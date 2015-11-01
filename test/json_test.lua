function equals(o1, o2)
    if o1 == o2 then return true end
    local o1Type = type(o1)
    local o2Type = type(o2)
    if o1Type ~= o2Type then return false end
    if o1Type ~= 'table' then return false end

    local keySet = {}

    for key1, value1 in pairs(o1) do
        local value2 = o2[key1]
        if value2 == nil or equals(value1, value2) == false then
            return false
        end
        keySet[key1] = true
    end

    for key2, _ in pairs(o2) do
        if not keySet[key2] then return false end
    end
    return true
end

a1 = {awesome = true, 
      library = "jsx"}
a2 = "{\"awesome\":true,\"library\":\"jsx\"}"
assert(equals(a1, json.decode(a2)))
assert(equals(json.encode(a1), a2))

b1 = {"a", "list", "of", "words"}
b2 = "[\"a\",\"list\",\"of\",\"words\"]"
assert(equals(b1, json.decode(b2)))
assert(equals(json.encode(b1), b2))

c1 = {a = {b = {c = "hello", d = {1, 2, 3, 4, 5}}}}
c2 = "{\"a\":{\"b\":{\"c\":\"hello\",\"d\":[1.0,2.0,3.0,4.0,5.0]}}}"
assert(equals(c1, json.decode(c2)))
assert(equals(json.encode(c1), c2))

