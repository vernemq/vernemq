-module(webhooks_handler).
-include("vmq_webhooks_test.hrl").

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

-export([start_endpoint/0,
         stop_endpoint/0]).

start_endpoint() ->
    Dispatch = cowboy_router:compile(
                 [{'_', [{"/", ?MODULE, []}]}]),
    {ok, _} = cowboy:start_http(http, 1, [{port, 34567}],
        [{env, [{dispatch, Dispatch}]}]
    ).

stop_endpoint() ->
    cowboy:stop_listener(http).

%% Cowboy callbacks
init(_Type, Req, []) ->
	{ok, Req, undefined}.

handle(Req, State) ->
    {Hook, Req2} = cowboy_req:header(<<"vernemq-hook">>, Req),
    {ok, Body, Req3} = cowboy_req:body(Req2),
    {Code, Resp} = process_hook(Hook, jsx:decode(Body, [{labels, atom}, return_maps])),
    {ok, Req4} =
        cowboy_req:reply(Code,
                         [
                          {<<"content-type">>, <<"text/json">>}
                         ], jsx:encode(Resp), Req3),
    {ok, Req4, State}.

%% callbacks for each hook
auth_on_register(#{peer_addr := ?PEER_BIN,
                   peer_port := ?PEERPORT,
                   subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
                   mountpoint := ?MOUNTPOINT_BIN,
                   username := ?USERNAME,
                   password := ?PASSWORD,
                   clean_session := true
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_register(#{subscriber_id := ?NOT_ALLOWED_SUBSCRIBER_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_register(#{subscriber_id := ?IGNORED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_register(#{subscriber_id := ?CHANGED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{mountpoint => <<"mynewmount">>}}};
auth_on_register(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_publish(#{username := ?USERNAME,
                  subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
                  mountpoint := ?MOUNTPOINT_BIN,
                  qos := 1,
                  topic := ?TOPIC,
                  payload := ?PAYLOAD,
                  retain := false
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_publish(#{subscriber_id := ?NOT_ALLOWED_SUBSCRIBER_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_publish(#{subscriber_id := ?IGNORED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_publish(#{subscriber_id := ?CHANGED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{topic => <<"rewritten/topic">>}}};
auth_on_publish(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_subscribe(#{username := ?USERNAME,
                    subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
                    mountpoint := ?MOUNTPOINT_BIN,
                    topics := [#{topic := ?TOPIC, qos := 1}]
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_subscribe(#{subscriber_id := ?NOT_ALLOWED_SUBSCRIBER_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_subscribe(#{subscriber_id := ?IGNORED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_subscribe(#{subscriber_id := ?CHANGED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"ok">>,
            topics =>
                [#{topic => <<"rewritten/topic">>,
                   qos => 2}]}};
auth_on_subscribe(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

on_register(#{peer_addr := ?PEER_BIN,
              peer_port := ?PEERPORT,
              mountpoint := ?MOUNTPOINT_BIN,
              subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
              username := BinPid}) -> 
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_register_ok,
    {200, #{}}.

on_publish(#{username := BinPid,
             mountpoint := ?MOUNTPOINT_BIN,
             subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
             topic := ?TOPIC,
             qos := 1,
             payload := ?PAYLOAD,
             retain := false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_publish_ok,
    {200, #{}}.

on_subscribe(#{username := BinPid,
               mountpoint := ?MOUNTPOINT_BIN,
               subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
               topics := [#{topic := ?TOPIC, qos := 1}]
              }) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_subscribe_ok,
    {200, #{}}.

on_unsubscribe(#{subscriber_id := ?ALLOWED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"ok">>}};
on_unsubscribe(#{subscriber_id := ?CHANGED_SUBSCRIBER_ID}) ->
    {200, #{result => <<"ok">>,
            topics => [<<"rewritten/topic">>]}}.

on_deliver(#{username := BinPid,
             mountpoint := ?MOUNTPOINT_BIN,
             subscriber_id := ?ALLOWED_SUBSCRIBER_ID,
             topic := ?TOPIC,
             payload := ?PAYLOAD}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_deliver_ok,
    {200, #{result => <<"ok">>}}.

on_offline_message(#{mountpoint := ?MOUNTPOINT_BIN,
                     subscriber_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_offline_message_ok,
    {200, #{}}.

on_client_wakeup(#{mountpoint := ?MOUNTPOINT_BIN,
                   subscriber_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_wakeup_ok,
    {200, #{}}.

on_client_offline(#{mountpoint := ?MOUNTPOINT_BIN,
                   subscriber_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_offline_ok,
    {200, #{}}.

on_client_gone(#{mountpoint := ?MOUNTPOINT_BIN,
                 subscriber_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_gone_ok,
    {200, #{}}.

terminate(_Reason, _Req, _State) ->
	ok.

process_hook(<<"auth_on_register">>, Body) ->
    auth_on_register(Body);
process_hook(<<"auth_on_publish">>, Body) ->
    auth_on_publish(Body);
process_hook(<<"auth_on_subscribe">>, Body) ->
    auth_on_subscribe(Body);
process_hook(<<"on_register">>, Body) ->
    on_register(Body);
process_hook(<<"on_publish">>, Body) ->
    on_publish(Body);
process_hook(<<"on_subscribe">>, Body) ->
    on_subscribe(Body);
process_hook(<<"on_unsubscribe">>, Body) ->
    on_unsubscribe(Body);
process_hook(<<"on_deliver">>, Body) ->
    on_deliver(Body);
process_hook(<<"on_offline_message">>, Body) ->
    on_offline_message(Body);
process_hook(<<"on_client_wakeup">>, Body) ->
    on_client_wakeup(Body);
process_hook(<<"on_client_offline">>, Body) ->
    on_client_offline(Body);
process_hook(<<"on_client_gone">>, Body) ->
    on_client_gone(Body).
