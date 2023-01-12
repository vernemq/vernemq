-module(vmq_generic_offline_msg_store_SUITE).
-include_lib("vmq_commons/src/vmq_types_common.hrl").
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0,
         groups/0
        ]).

-export([insert_delete_test/1                                                                                                                     ]).


%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(StorageEngine, Config) ->
    [{engine, StorageEngine}|Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    StorageEngine = proplists:get_value(engine, Config),
    Opts = case StorageEngine of
               vmq_offline_storage_engine_postgres ->
                   [
                      {username, vmq_test_user},
                      {password, vmq_test_password},
                      {database, "vmq_test_database"},
                      {host, "localhost"},
                      {port, 5432}
                   ];
               vmq_offline_storage_engine_redis ->
                   {ok, _} = vmq_metrics:start_link(),
                   [
                       {database, "2"},
                       {host, "[\"localhost\"]"},
                       {port, 26379}
                   ]
           end,
    application:load(vmq_generic_offline_msg_store),
    application:set_env(vmq_generic_offline_msg_store, msg_store_engine, StorageEngine),
    application:set_env(vmq_generic_offline_msg_store, msg_store_opts, Opts),
    application:ensure_all_started(vmq_generic_offline_msg_store),
    Config.

end_per_testcase(_, Config) ->
    application:stop(vmq_generic_offline_msg_store),
    Config.

all() ->
    [
     {group, vmq_offline_storage_engine_postgres},
     {group, vmq_offline_storage_engine_redis}
    ].

groups() ->
    [
     {vmq_offline_storage_engine_postgres, [], [insert_delete_test]},
     {vmq_offline_storage_engine_redis, [], [insert_delete_test]}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
insert_delete_test(Config) ->
    SId = {"", "foo"},

    {0, []} = store_summary(SId),

    Msgs = generate_msgs(1000, []),
    ok = store_msgs({"", "foo"}, Msgs), %% write

    {1000, [#deliver{msg = Msg} | _]} = store_summary(SId), %% find

    {ok, _} = vmq_generic_offline_msg_store:msg_store_delete(SId, Msg#vmq_msg.msg_ref), %% delete
    {ok, _} = vmq_generic_offline_msg_store:msg_store_delete(SId), %% delete all

    {0, []} = store_summary(SId), %% find
    Config.

generate_msgs(0, Acc) -> Acc;
generate_msgs(N, Acc) ->
    Msg = #vmq_msg{msg_ref= msg_ref(),
                   routing_key= rand_bytes(10),
                   payload = rand_bytes(100),
                   mountpoint = "",
                   dup = random_flag(),
                   qos = random_qos(),
                   properties=#{},
                   persisted=true},
    generate_msgs(N - 1, [Msg|Acc]).

store_msgs(SId, [Msg|Rest]) ->
    {ok, _} = vmq_generic_offline_msg_store:msg_store_write(SId, Msg),
    store_msgs(SId, Rest);
store_msgs(_, []) -> ok.

random_flag() ->
    rand:uniform(10) > 5.

random_qos() ->
    rand:uniform(3) - 1.

store_summary(SId) ->
    {ok, MsgList} = vmq_generic_offline_msg_store:msg_store_find(SId),
    Size = size(MsgList, 0),
    {Size, MsgList}.

size([], S) -> S;
size([_ | R], S) -> size(R, S + 1).

rand_bytes(N) ->
    crypto:strong_rand_bytes(N).

msg_ref() ->
    erlang:md5(term_to_binary({node(), self(), erlang:timestamp(), rand_bytes(10)})).
