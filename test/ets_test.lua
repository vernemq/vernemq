kv.ensure_table({name = "kv_test"})

assert(kv.delete_all("kv_test"))
assert(kv.insert("kv_test", {hello = "world"}))
assert(kv.insert_new("kv_test", {hello = "earth"}) == false)
assert(kv.insert("kv_test", {hello = "earth"}))
ret = kv.lookup("kv_test", "hello")
assert(ret[1] == "earth")

obj = {a = {b = {c = {d = "hello"}}}}
assert(kv.insert("kv_test", obj))
assert(kv.lookup("kv_test", "a")[1].b.c.d, "hello")

assert(kv.delete("kv_test", "a"))
assert(kv.delete("kv_test", "hello"))
