config = {
    pool_id = "postgres_test",
    user = "vmq_test_user",
    password = "vmq_test_password",
    database = "vmq_test_database"
}

postgres.ensure_pool(config)

-- Drop test table
assert(postgres.execute("postgres_test", "DROP TABLE IF EXISTS postgres_test_lua_tbl"))

-- Create test table
assert(postgres.execute("postgres_test", 
    [[CREATE TABLE postgres_test_lua_tbl(
        client_id VARCHAR(128) PRIMARY KEY,
        mountpoint VARCHAR(64),
        user_name VARCHAR(128),
        password VARCHAR(128)
     )
     ]]))

-- insert some data
assert(postgres.execute("postgres_test", 
    [[INSERT INTO postgres_test_lua_tbl VALUES
        ('client_a', 'my_mp', 'user_a', 'password_a'),
        ('client_b', 'my_mp', 'user_b', 'password_b'),
        ('client_c', 'my_mp', 'user_c', 'password_c'),
        ('client_d', 'my_mp', 'user_d', 'password_d')
    ]]))

-- select
results = postgres.execute("postgres_test", "SELECT * FROM postgres_test_lua_tbl")
assert(#results == 4, "error in select")

function assert_row(row, client_id, mountpoint, user_name, password)
    assert(row.client_id == client_id)
    assert(row.mountpoint == mountpoint)
    assert(row.user_name == user_name)
    assert(row.password == password)
end

function assert_result(results)
    assert_row(results[1], 'client_a', 'my_mp', 'user_a', 'password_a')
    assert_row(results[2], 'client_b', 'my_mp', 'user_b', 'password_b')
    assert_row(results[3], 'client_c', 'my_mp', 'user_c', 'password_c')
    assert_row(results[4], 'client_d', 'my_mp', 'user_d', 'password_d')
end

-- assert result rows
assert_result(results)

-- same with prepared select statement
--assert(postgres.execute("postgres_test", "PREPARE select_all AS SELECT * FROM postgres_test_lua_tbl"))
--
--results = postgres.execute("postgres_test", "EXECUTE select_all")
--assert(#results == 4, "error in select")
--assert_result(results)


-- more complex query
results = postgres.execute("postgres_test", 
    [[SELECT * FROM postgres_test_lua_tbl 
      WHERE 
        mountpoint=$1 and
        client_id=$2 and
        user_name=$3]], 'my_mp', 'client_c', 'user_c')

assert(#results == 1, "error in select")
