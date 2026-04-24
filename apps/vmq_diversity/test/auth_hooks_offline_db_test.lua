config = {
    pool_id = "postgres_auth_hooks_offline_test",
    user = "vmq_test_user",
    password = "vmq_test_password",
    database = "vmq_test_database",
    -- hopefully there's no postgresql instance on this port
    port = 12345
}

postgres.ensure_pool(config)

function fail_with_offline_db()
    postgres.execute("postgres_auth_hooks_offline_test", "SELECT 1")
end

function auth_on_register(reg)
    fail_with_offline_db()
    return true
end

function auth_on_publish(pub)
    fail_with_offline_db()
    return true
end

function auth_on_subscribe(sub)
    fail_with_offline_db()
    return true
end

function auth_on_register_m5(reg)
    fail_with_offline_db()
    return true
end

function auth_on_publish_m5(pub)
    fail_with_offline_db()
    return true
end

function auth_on_subscribe_m5(sub)
    fail_with_offline_db()
    return true
end

hooks = {
    auth_on_register = auth_on_register,
    auth_on_publish = auth_on_publish,
    auth_on_subscribe = auth_on_subscribe,
    auth_on_register_m5 = auth_on_register_m5,
    auth_on_publish_m5 = auth_on_publish_m5,
    auth_on_subscribe_m5 = auth_on_subscribe_m5
}
