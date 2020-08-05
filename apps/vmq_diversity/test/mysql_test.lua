-- Pool Setup
config = {
    pool_id = "mysql_test",
    size = 5,
    user = "vmq_test_user",
    password = "vmq_test_password",
    database = "vmq_test_database",
    host = "localhost",
    port = 3306
}
assert(mysql.ensure_pool(config))

-- Drop test table
assert(mysql.execute("mysql_test", "DROP TABLE IF EXISTS mysql_test_lua_tbl"))

-- Create test table
assert(mysql.execute("mysql_test", 
    [[CREATE TABLE mysql_test_lua_tbl(
        client_id VARCHAR(128) PRIMARY KEY,
        mountpoint VARCHAR(64),
        user_name VARCHAR(128),
        password VARCHAR(128)
     )
     ]]))

-- insert some data
assert(mysql.execute("mysql_test", 
    [[INSERT INTO mysql_test_lua_tbl VALUES
        ('client_a', 'my_mp', 'user_a', 'password_a'),
        ('client_b', 'my_mp', 'user_b', 'password_b'),
        ('client_c', 'my_mp', 'user_c', 'password_c'),
        ('client_d', 'my_mp', 'user_d', 'password_d')
    ]]))

-- select
results = mysql.execute("mysql_test", "SELECT * FROM mysql_test_lua_tbl")
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
--assert(mysql.execute("mysql_test", "PREPARE select_all FROM 'SELECT * FROM mysql_test_lua_tbl'"))
--results = mysql.execute("mysql_test", "EXECUTE select_all")
--print(results)
-- assert(#results == 4, "error in select")
--assert_result(results)


-- more complex query
results = mysql.execute("mysql_test", 
    [[SELECT * FROM mysql_test_lua_tbl 
      WHERE 
        mountpoint=? &&
        client_id=? &&
        user_name=?]], 'my_mp', 'client_c', 'user_c')

assert(#results == 1, "error in select")

-- Test the default hashing method
assert(mysql.hash_method() == "PASSWORD(?)")
