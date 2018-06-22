-module(vmq_lvldb_store_SUITE).
-include("src/vmq_server.hrl").
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([insert_delete_test/1,
         ref_delete_test/1,
         message_compat_pre_test/1,
         idx_compat_pre_test/1]).


%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    %% this might help, might not...
    cover:start(),
    [S|Config].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(message_compat_pre_test, Config) ->
    Config;
init_per_testcase(idx_compat_pre_test, Config) ->
    Config;
init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    vmq_test_utils:setup(),
    Config.

end_per_testcase(message_compat_pre_test, Config) ->
    Config;
end_per_testcase(idx_compat_pre_test, Config) ->
    Config;
end_per_testcase(_, Config) ->
    vmq_server:teardown(),
    Config.

all() ->
    [insert_delete_test,
     ref_delete_test,
     message_compat_pre_test,
     idx_compat_pre_test].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
insert_delete_test(Config) ->
    {0,0,0} = store_summary(),
    Msgs = generate_msgs(1000, []),
    Refs = [Ref || #vmq_msg{msg_ref=Ref} <- Msgs],
    ok = store_msgs({"", "foo"}, Msgs),

    {1000,1000,1000} = store_summary(),

    1000 = refcount(Refs, 0),
    %% we should get back the exact same list
    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo"}),
    %% delete all
    ok = delete_msgs({"", "foo"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo"}),

    0 = refcount(Refs, 0),
    {0,0,0} = store_summary(),
    Config.

ref_delete_test(Config) ->
    {0,0,0} = store_summary(),
    Msgs = generate_msgs(1000, []),
    Refs = [Ref || #vmq_msg{msg_ref=Ref} <- Msgs],
    ok = store_msgs({"", "foo0"}, Msgs),
    ok = store_msgs({"", "foo1"}, Msgs),
    ok = store_msgs({"", "foo2"}, Msgs),
    ok = store_msgs({"", "foo3"}, Msgs),
    ok = store_msgs({"", "foo4"}, Msgs),
    ok = store_msgs({"", "foo5"}, Msgs),
    ok = store_msgs({"", "foo6"}, Msgs),
    ok = store_msgs({"", "foo7"}, Msgs),
    ok = store_msgs({"", "foo8"}, Msgs),
    ok = store_msgs({"", "foo9"}, Msgs),

    10000 = refcount(Refs, 0),
    {1000,10000,10000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo0"}),
    {ok, Msgs} = read_msgs({"", "foo0"}, Refs),
    ok = delete_msgs({"", "foo0"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo0"}),

    9000 = refcount(Refs, 0),
    {1000,9000,9000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo1"}),
    {ok, Msgs} = read_msgs({"", "foo1"}, Refs),
    ok = delete_msgs({"", "foo1"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo1"}),

    8000 = refcount(Refs, 0),
    {1000,8000,8000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo2"}),
    {ok, Msgs} = read_msgs({"", "foo2"}, Refs),
    ok = delete_msgs({"", "foo2"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo2"}),

    7000 = refcount(Refs, 0),
    {1000,7000,7000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo3"}),
    {ok, Msgs} = read_msgs({"", "foo3"}, Refs),
    ok = delete_msgs({"", "foo3"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo3"}),

    6000 = refcount(Refs, 0),
    {1000,6000,6000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo4"}),
    {ok, Msgs} = read_msgs({"", "foo4"}, Refs),
    ok = delete_msgs({"", "foo4"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo4"}),

    5000 = refcount(Refs, 0),
    {1000,5000,5000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo5"}),
    {ok, Msgs} = read_msgs({"", "foo5"}, Refs),
    ok = delete_msgs({"", "foo5"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo5"}),

    4000 = refcount(Refs, 0),
    {1000,4000,4000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo6"}),
    {ok, Msgs} = read_msgs({"", "foo6"}, Refs),
    ok = delete_msgs({"", "foo6"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo6"}),

    3000 = refcount(Refs, 0),
    {1000,3000,3000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo7"}),
    {ok, Msgs} = read_msgs({"", "foo7"}, Refs),
    ok = delete_msgs({"", "foo7"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo7"}),

    2000 = refcount(Refs, 0),
    {1000,2000,2000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo8"}),
    {ok, Msgs} = read_msgs({"", "foo8"}, Refs),
    ok = delete_msgs({"", "foo8"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo8"}),

    1000 = refcount(Refs, 0),
    {1000,1000,1000} = store_summary(),

    {ok, Refs} = vmq_lvldb_store:msg_store_find({"", "foo9"}),
    {ok, Msgs} = read_msgs({"", "foo9"}, Refs),
    ok = delete_msgs({"", "foo9"}, Msgs),
    {ok, []} = vmq_lvldb_store:msg_store_find({"", "foo9"}),

    0 = refcount(Refs, 0),
    {0,0,0} = store_summary(),

    Config.

%% @doc test that the message store functions can parse and serialize
%% data from the future as well as the pre versioning format.
message_compat_pre_test(_Cfg) ->
    %% We can serialize and parse msg vals from before versioning was added
    PreVersion = {[<<"routing">>, <<"key">>], %% routing_key = [<<"routing">>, <<"key">>]
                   <<"payload">>},
    PreVersion = vmq_lvldb_store:parse_p_msg_val_pre(
                   vmq_lvldb_store:serialize_p_msg_val_pre(PreVersion)),

    %% We can also serialize / parse something from the future:
    FutureVersion = {1, %% version
                     [<<"routing">>, <<"key">>], <<"payload">>, <<"something else">>},

    PreVersion = vmq_lvldb_store:parse_p_msg_val_pre(
                   vmq_lvldb_store:serialize_p_msg_val_pre(FutureVersion)),
    ok.

idx_compat_pre_test(_Cfg) ->
    %% We can serialize and parse idx vals from before versioning was added
    PreVersion = {p_idx_val,
                  {1529,586954,257209}, %% ts = {1529,586954,257209}
                  false, %% dup = false
                  2 %% qos = 2
                 },
    PreVersion = vmq_lvldb_store:parse_p_idx_val_pre(
                   vmq_lvldb_store:serialize_p_idx_val_pre(PreVersion)),

    %% We can also serialize / parse something from the future:
    FutureVersion = {p_idx_val,
                     1, %% version = 1
                     {1529,586954,257209}, %% ts = {1529,586954,257209}
                     false, %% dup = false
                     2, %% qos = 2
                     "something unknown1",
                     "something unknown2"
                    },

    PreVersion = vmq_lvldb_store:parse_p_idx_val_pre(
                   vmq_lvldb_store:serialize_p_idx_val_pre(FutureVersion)),
    ok.

generate_msgs(0, Acc) -> Acc;
generate_msgs(N, Acc) ->
    Msg = #vmq_msg{msg_ref=vmq_mqtt_fsm_util:msg_ref(),
                   routing_key=vmq_test_utils:rand_bytes(10),
                   payload = vmq_test_utils:rand_bytes(100),
                   mountpoint = "",
                   dup = random_flag(),
                   qos = random_qos(),
                   properties=#{},
                   persisted=true},
    generate_msgs(N - 1, [Msg|Acc]).

store_msgs(SId, [Msg|Rest]) ->
    ok = vmq_lvldb_store:msg_store_write(SId, Msg),
    store_msgs(SId, Rest);
store_msgs(_, []) -> ok.

delete_msgs(_, []) -> ok;
delete_msgs(SId, [#vmq_msg{msg_ref=Ref}|Rest]) ->
    ok = vmq_lvldb_store:msg_store_delete(SId, Ref),
    delete_msgs(SId, Rest).

read_msgs(SId, Refs) ->
    read_msgs(SId, Refs, []).
read_msgs(_, [], Acc) -> {ok, lists:reverse(Acc)};
read_msgs(SId, [Ref|Refs], Acc) ->
    {ok, Msg} = vmq_lvldb_store:msg_store_read(SId, Ref),
    read_msgs(SId, Refs, [Msg|Acc]).

refcount([Ref|Refs], Cnt) ->
    refcount(Refs, Cnt + vmq_lvldb_store:refcount(Ref));
refcount([], Cnt) -> Cnt.


random_flag() ->
    rand:uniform(10) > 5.

random_qos() ->
    rand:uniform(3) - 1.

store_summary() ->
    vmq_lvldb_store_utils:full_table_scan(
      fun
          ({msg, _, _, _, _}, {NumMsgs, NumRefs, NumIdxs}) ->
              {NumMsgs + 1, NumRefs, NumIdxs};
          ({ref, _, _, _}, {NumMsgs, NumRefs, NumIdxs}) ->
              {NumMsgs, NumRefs + 1, NumIdxs};
          ({idx, _, _, _, _}, {NumMsgs, NumRefs, NumIdxs}) ->
              {NumMsgs, NumRefs, NumIdxs + 1}
      end, {0,0,0}).

