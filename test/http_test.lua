P = "http_test"
ret = http.post(P, "http://www.google.com", "hello world", {x_post_header = "X-POST-HEADER"})
assert(ret.status)
assert(ret.ref)
body = http.body(ret.ref)
assert(body)
