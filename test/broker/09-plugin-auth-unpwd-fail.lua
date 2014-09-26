function on_register(client_id, user, password)
    if user == "test-username" 
        and not password == "cnwTICONIURW" then
        return true
    elseif user == "readonly" then
        return true
    else
        return false
    end

end

function on_subscribe(client_id, user, topic, qos)
    if user == "readonly" then
        return true
    else
        return false
    end

end
