ret = vmq_api.disconnect_by_subscriber_id({mountpoint = "mp",
                                           client_id = "client-id"}, {do_cleanup = true})
assert(ret == "not_found")
