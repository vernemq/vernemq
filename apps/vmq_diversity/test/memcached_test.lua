memcached.ensure_pool({pool_id = "mcd_test"})

assert(memcached.flush_all("mcd_test") == true)

-- test get/set
assert(memcached.get("mcd_test", "unknown key") == false)
assert(memcached.set("mcd_test", "hello", "world") == "world")
assert(memcached.get("mcd_test", "hello") == "world")

-- test delete
assert(memcached.delete("mcd_test", "hello") == true)
assert(memcached.delete("mcd_test", "hello") == false)
assert(memcached.get("mcd_test", "hello") == false)

-- test add
assert(memcached.add("mcd_test", "add_sth", "sth") == "sth")
assert(memcached.add("mcd_test", "add_sth", "sth") == false)

-- test replace
assert(memcached.set("mcd_test", "replaceme", "val") == "val")
assert(memcached.replace("mcd_test", "replaceme", "replaced") == "replaced")
assert(memcached.get("mcd_test", "replaceme") == "replaced")
assert(memcached.replace("mcd_test", "cantreplaceme", "whatever") == false)
assert(memcached.get("mcd_test", "cantreplaceme") == false)

-- -- test funcs /w expiration
assert(memcached.set("mcd_test", "replace_exp", "val") == "val")
assert(memcached.replace("mcd_test", "replace_exp", "replaced", 1) == "replaced")
assert(memcached.get("mcd_test", "replace_exp") == "replaced")

assert(memcached.add("mcd_test", "add_exp", "val", 1) == "val")
assert(memcached.get("mcd_test", "add_exp") == "val")

assert(memcached.set("mcd_test", "set_exp", "val", 1) == "val")
assert(memcached.get("mcd_test", "set_exp") == "val")

local clock = os.clock
function sleep(n)  -- seconds
  local t0 = clock()
  while clock() - t0 <= n do end
end

sleep(2)
assert(memcached.get("mcd_test", "replace_exp") == false)
assert(memcached.get("mcd_test", "add_exp") == false)
assert(memcached.get("mcd_test", "set_exp") == false)

assert(memcached.flush_all("mcd_test") == true)




