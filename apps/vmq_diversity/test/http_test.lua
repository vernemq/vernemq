http.ensure_pool({pool_id = "http_test"})

ret = http.post("http_test", "http://www.google.com", "hello world", {x_post_header = "X-POST-HEADER"})
assert(ret.status)
assert(ret.ref)
body = http.body(ret.ref)
assert(body)
assert(body == ret.ref)
