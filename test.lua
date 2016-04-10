mongodb.ensure_pool({size = 1, w_mode = "safe"})

function myhook(reg)
    log.info("got connect attempt from " .. reg.client_id)
    return true

end

hooks = {
    auth_on_register = myhook
}

-- redis auth
--redis.cmd("redis_test", "set client_a my_password")
--function auth_on_register(args)
--    assert(args.nana)
--    if args.client_id and redis.cmd("redis_test", "get " .. args.client_id) == args.password then
--        return true
--    else
--        return false
--    end
--end

-- postgres auth
--function auth_on_register(args)
--    ret = pgsql.execute("postgres_test", "SELECT * FROM postgres_test_lua_tbl WHERE client_id=$1", args.client_id)
--    if #ret then
--        if args.client_id and ret[1].password == args.password then
--            return true
--        else
--            return false
--        end
--    end
--end

-- mongodb auth
--pcall(
--function() 
--    mongodb.insert("mongodb_test", "users", {client_id="client_a", password="password_a"})
--end)
--pcall(
--function() 
--    mongodb.insert("mongodb_test", "users", {client_id="client_b", password="password_b"})
--end)
--function auth_on_register(args)
--    ret = mongodb.find_one("mongodb_test", "users", {client_id=args.client_id})
--    --print(ret.client_id)
--    print("--------auth_on_register-----------")
--    ptable(args)
--    if ret.password == args.password then
--        return true
--    else
--        return false
--    end
--end
--
--function auth_on_subscribe(args)
--    print("--------auth_on_subscribe-----------")
--    ptable(args)
--    for topic, qos in pairs(args.topics) do
--        print("--", topic, qos)
--        --print("subscribe to", fmt_topic(topic), qos)
--    end
--    --rewrite topic
--    topics = {}
--    topics["good/bye"] = 1
--    return topics
--end
--
--function auth_on_publish(args)
--    print("--------auth_on_publish-----------")
--    ptable(args)
--
--    -- rewrite topic
--    return {topic = "good/bye", qos = 1}
--end
--
--function on_register(args)
--    print("--------on_register-----------")
--    ptable(args)
--end
--
--function on_subscribe(args)
--    print("--------on_subscribe-----------")
--    ptable(args)
--end
--
--function on_publish(args)
--    print("--------on_publish-----------")
--    ptable(args)
--end
--
--function on_deliver(args)
--    print("--------on_deliver-----------")
--    ptable(args)
--    --rewrite
--    args.topic = "hello/world"
--    args.payload = "haha rewrite"
--    return args
--end
--
--
--function ptable(args)
--    ptable_("", args)
--end
--function ptable_(prefix, args)
--    for k,v in pairs(args) do
--        if type(v) == 'table' then
--            print(prefix, k, ":")
--            new_prefix = prefix .. "  "
--            ptable_(new_prefix, v)
--        else
--            print(prefix, k, v)
--        end
--    end
--end
--
--hooks = {auth_on_register=auth_on_register,
--         auth_on_subscribe=auth_on_subscribe,
--         auth_on_publish=auth_on_publish,
--         on_register=on_register,
--         on_subscribe=on_subscribe,
--         on_publish=on_publish,
--         on_deliver=on_deliver}
