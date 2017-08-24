config = {
    pool_id = "postgres_test",
    user = "vmq_test_user",
    password = "vmq_test_password",
    database = "vmq_test_database",
    -- hopefully there's no postgresql instance on this port
    port = 12345
}

postgres.ensure_pool(config)

function auth_on_register(reg)
   -- if not connected to postgres this should abort the script and
   -- return an error to the hook caller.
   postgres.execute("postgres_test", "DROP TABLE IF EXISTS postgres_test_lua_tbl")
   return true
end

hooks = {
    auth_on_register = auth_on_register
}
