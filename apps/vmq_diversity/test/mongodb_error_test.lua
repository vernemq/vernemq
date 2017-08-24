config = {
    pool_id = "mongodb_test",
    size = 1,
    w_mode = "safe",
    -- hopefully there's no monbodb instance on this port
    port = 12345
}

assert(mongodb.ensure_pool(config))

function auth_on_register(reg)
   -- if not connected to mongo this should abort the script and
   -- return an error to the hook caller.
   mongodb.delete("mongodb_test", "users", {})
   return true
end

hooks = {
    auth_on_register = auth_on_register
}
