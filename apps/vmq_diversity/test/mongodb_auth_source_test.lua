config = {
    pool_id = "mongodb_test",
    size = 1,
    database="admin",
    auth_source="core",
    login="vmq_auth_source_test_user",
    password="vmq_auth_source_test_password",
    w_mode = "safe"
}

assert(mongodb.ensure_pool(config))

assert(mongodb.delete("mongodb_test", "users", {}))

doc1 = {_id = 'test-id',
       first_name = 'Jules',
       last_name = 'Verne',
       protocol = 'MQTT',
       broker = 'VerneMQ'}

-- insert one document
ret = mongodb.insert("mongodb_test", "users", doc1)
assert(ret._id ==  'test-id')
