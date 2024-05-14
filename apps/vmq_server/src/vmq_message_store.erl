-module(vmq_message_store).

-behaviour(supervisor).

-include("vmq_server.hrl").

%% Supervisor callbacks
-export([init/1]).

%% API
-export([
    start/0,
    write/2,
    read/2,
    delete/1,
    delete/2,
    find/1
]).

start() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

write(SubscriberId, Msg) ->
    vmq_redis:query(
        vmq_message_store_redis_client,
        ["RPUSH", term_to_binary(SubscriberId), term_to_binary(Msg)],
        ?RPUSH,
        ?MSG_STORE_WRITE
    ).

read(_SubscriberId, _MsgRef) ->
    {error, not_supported}.

delete(SubscriberId) ->
    vmq_redis:query(
        vmq_message_store_redis_client,
        ["DEL", term_to_binary(SubscriberId)],
        ?DEL,
        ?MSG_STORE_DELETE
    ).

delete(SubscriberId, _MsgRef) ->
    vmq_redis:query(
        vmq_message_store_redis_client,
        ["LPOP", term_to_binary(SubscriberId), 1],
        ?LPOP,
        ?MSG_STORE_DELETE
    ).

find(SubscriberId) ->
    case
        vmq_redis:query(
            vmq_message_store_redis_client,
            ["LRANGE", term_to_binary(SubscriberId), "0", "-1"],
            ?FIND,
            ?MSG_STORE_FIND
        )
    of
        {ok, MsgsInB} ->
            DMsgs = lists:foldr(
                fun(MsgB, Acc) ->
                    Msg = binary_to_term(MsgB),
                    D = #deliver{msg = Msg, qos = Msg#vmq_msg.qos},
                    [D | Acc]
                end,
                [],
                MsgsInB
            ),
            {ok, DMsgs};
        Res ->
            Res
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init([]) ->
    {'ok',
        {{'one_for_one', 5, 10}, [
            {atom(), {atom(), atom(), list()}, permanent, pos_integer(), worker, [atom()]}
        ]}}.
init([]) ->
    StoreCfgs = application:get_env(vmq_server, message_store, [
        {redis, [
            {connect_options, "[{sentinel, [{endpoints, [{\"localhost\", 26379}]}]},{database,2}]"}
        ]}
    ]),
    Redis = proplists:get_value(redis, StoreCfgs),
    Username =
        case proplists:get_value(username, Redis, undefined) of
            undefined -> undefined;
            User when is_atom(User) -> atom_to_list(User)
        end,
    Password =
        case proplists:get_value(password, Redis, undefined) of
            undefined -> undefined;
            Pass when is_atom(Pass) -> atom_to_list(Pass)
        end,

    {ok,
        {{one_for_one, 5, 10}, [
            {eredis,
                {eredis, start_link, [
                    [
                        {username, Username},
                        {password, Password},
                        {name, {local, vmq_message_store_redis_client}}
                        | vmq_schema_util:parse_list(proplists:get_value(connect_options, Redis))
                    ]
                ]},
                permanent, 5000, worker, [eredis]}
        ]}}.
