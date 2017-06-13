keep_state=false
num_states=100


function balanced_function()
    return __SCRIPT_INSTANCE_ID__
end

hooks = {
    balanced_function = balanced_function
}
