-module(webhooks_handler).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").
-include("vmq_webhooks_test.hrl").

-export([init/2]).
-export([terminate/3]).

-export([start_endpoint_clear/1,
         start_endpoint_tls/1,
         stop_endpoint_clear/0,
         stop_endpoint_tls/0]).

-define(DEBUG, false).

start_endpoint_tls(#{port:= _HTTPSPort,
                     keyfile:= _KeyFile,
                     certfile:= _CertFile,
                     cacertfile:= _CACertFile} = SocketOpts) ->
    RanchOpts = maps:to_list(SocketOpts),
    {ok, _} = cowboy:start_tls(https, RanchOpts, #{env => #{dispatch => route()}}),
    ok.

start_endpoint_clear(HTTPPort) ->
    {ok, _} = cowboy:start_clear(http, [{port, HTTPPort}],
                                 #{env => #{dispatch => route()}}).

route() -> cowboy_router:compile(
                 [{'_', [{"/", ?MODULE, []},
                         {"/cache", ?MODULE, []},
                         {"/cache1s", ?MODULE, []}]}]).

stop_endpoint_tls() ->
    cowboy:stop_listener(https).
stop_endpoint_clear() ->
    cowboy:stop_listener(http).

%% Cowboy callbacks
init(Req, State) ->
    Hook = cowboy_req:header(<<"vernemq-hook">>, Req),
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    ?DEBUG andalso io:format(user, ">>> ~s~n", [Body]),
    case cowboy_req:path(Req) of
        <<"/">> ->
            {Code, Resp} = process_hook(Hook, vmq_json:decode(Body, [{labels, atom}, return_maps])),
            Req2 =
                cowboy_req:reply(Code,
                                 #{<<"content-type">> => <<"text/json">>},
                                 encode(Resp), Req1),
            {ok, Req2, State};
        <<"/cache">> ->
            {Code, Resp} = process_cache_hook(Hook, vmq_json:decode(Body, [{labels, atom}, return_maps])),
            Req2 =
                cowboy_req:reply(Code,
                                 #{<<"content-type">> => <<"text/json">>,
                                   <<"Cache-control">> => <<"max-age=86400">>},
                                 encode(Resp), Req1),
            {ok, Req2, State};
        <<"/cache1s">> ->
            {Code, Resp} = process_cache_hook(Hook, vmq_json:decode(Body, [{labels, atom}, return_maps])),
            Req2 =
                cowboy_req:reply(Code,
                                 #{<<"content-type">> => <<"text/json">>,
                                   <<"Cache-control">> => <<"max-age=1">>},
                                 encode(Resp), Req1),
            {ok, Req2, State}
    end.

encode(Term) ->
    Encoded = vmq_json:encode(Term),
    ?DEBUG andalso io:format(user, "<<< ~s~n", [Encoded]),
    Encoded.


process_cache_hook(<<"auth_on_register">>, #{username := SenderPid}) ->
    Pid = list_to_pid(binary_to_list(SenderPid)),
    Pid ! cache_auth_on_register_ok,
    {200, #{result => <<"ok">>}};
process_cache_hook(<<"auth_on_publish">>, #{username := SenderPid}) ->
    Pid = list_to_pid(binary_to_list(SenderPid)),
    Pid ! cache_auth_on_publish_ok,
    {200, #{result => <<"ok">>}};
process_cache_hook(<<"auth_on_subscribe">>, #{username := SenderPid}) ->
    Pid = list_to_pid(binary_to_list(SenderPid)),
    Pid ! cache_auth_on_subscribe_ok,
    {200, #{result => <<"ok">>}}.

%% callbacks for each hook
auth_on_register(#{peer_addr := ?PEER_BIN,
                   peer_port := ?PEERPORT,
                   client_id := <<"undefined_creds">>,
                   mountpoint := ?MOUNTPOINT_BIN,
                   username := null,
                   password := null,
                   clean_session := true
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_register(#{peer_addr := ?PEER_BIN,
                   peer_port := ?PEERPORT,
                   client_id := ?ALLOWED_CLIENT_ID,
                   mountpoint := ?MOUNTPOINT_BIN,
                   username := ?USERNAME,
                   password := ?PASSWORD,
                   clean_session := true
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_register(#{client_id := ?NOT_ALLOWED_CLIENT_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_register(#{client_id := ?IGNORED_CLIENT_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_register(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{mountpoint => <<"mynewmount">>,
                           client_id => <<"changed_client_id">>}}};
auth_on_register(#{username := ?CHANGED_USERNAME}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{username => <<"changed_username">>}}};
auth_on_register(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_register_m5(#{peer_addr := ?PEER_BIN,
                      peer_port := ?PEERPORT,
                      client_id := ?ALLOWED_CLIENT_ID,
                      mountpoint := ?MOUNTPOINT_BIN,
                      username := ?USERNAME,
                      password := ?PASSWORD,
                      clean_start := true
                     }) ->
    {200, #{result => <<"ok">>}};
auth_on_register_m5(#{client_id := ?NOT_ALLOWED_CLIENT_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_register_m5(#{client_id := ?IGNORED_CLIENT_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_register_m5(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{mountpoint => <<"mynewmount">>,
                           client_id => <<"changed_client_id">>}}};
auth_on_register_m5(#{username := ?CHANGED_USERNAME}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{username => <<"changed_username">>}}};
auth_on_register_m5(#{client_id := ?WITH_PROPERTIES,
                      properties :=
                          #{?P_SESSION_EXPIRY_INTERVAL := 5,
                            ?P_RECEIVE_MAX := 10,
                            ?P_TOPIC_ALIAS_MAX := 15,
                            ?P_REQUEST_RESPONSE_INFO := true,
                            ?P_REQUEST_PROBLEM_INFO := true,
                            ?P_USER_PROPERTY :=
                                [#{key := <<"azE=">>,val := <<"djE=">>},
                                 #{key := <<"azE=">>,val := <<"djI=">>},
                                 #{key := <<"azI=">>,val := <<"djI=">>}] = UPS}}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{properties => #{?P_USER_PROPERTY => UPS}}}};
auth_on_register_m5(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_publish(#{username := ?USERNAME,
                  client_id := ?ALLOWED_CLIENT_ID,
                  mountpoint := ?MOUNTPOINT_BIN,
                  qos := 1,
                  topic := ?TOPIC,
                  payload := ?PAYLOAD,
                  retain := false
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_publish(#{username := ?USERNAME,
                  client_id := ?BASE64_PAYLOAD_CLIENT_ID,
                  mountpoint := ?MOUNTPOINT_BIN,
                  qos := 1,
                  topic := ?TOPIC,
                  payload := Base64Payload,
                  retain := false
                 }) ->
    ?PAYLOAD = base64:decode(Base64Payload),
    {200, #{result => <<"ok">>,
            modifiers => #{payload => base64:encode(?PAYLOAD)}}};
auth_on_publish(#{username := ?USERNAME,
                  client_id := ?NO_PAYLOAD_CLIENT_ID,
                  mountpoint := ?MOUNTPOINT_BIN,
                  qos := 1,
                  topic := ?TOPIC,
                  retain := false
                 }=Args) ->
    false = maps:is_key(payload, Args),
    {200, #{result => <<"ok">>}};
auth_on_publish(#{client_id := ?NOT_ALLOWED_CLIENT_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_publish(#{client_id := ?IGNORED_CLIENT_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_publish(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{topic => <<"rewritten/topic">>}}};
auth_on_publish(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_publish_m5(#{username := ?USERNAME,
                     client_id := ?ALLOWED_CLIENT_ID,
                     mountpoint := ?MOUNTPOINT_BIN,
                     qos := 1,
                     topic := ?TOPIC,
                     payload := ?PAYLOAD,
                     retain := false
                    }) ->
    {200, #{result => <<"ok">>}};
auth_on_publish_m5(#{username := ?USERNAME,
                     client_id := ?BASE64_PAYLOAD_CLIENT_ID,
                     mountpoint := ?MOUNTPOINT_BIN,
                     qos := 1,
                     topic := ?TOPIC,
                     payload := Base64Payload,
                     retain := false
                    }) ->
    ?PAYLOAD = base64:decode(Base64Payload),
    {200, #{result => <<"ok">>,
            modifiers => #{payload => base64:encode(?PAYLOAD)}}};
auth_on_publish_m5(#{username := ?USERNAME,
                     client_id := ?NO_PAYLOAD_CLIENT_ID,
                     mountpoint := ?MOUNTPOINT_BIN,
                     qos := 1,
                     topic := ?TOPIC,
                     retain := false
                    }=Args) ->
    false = maps:is_key(payload, Args),
    {200, #{result => <<"ok">>}};
auth_on_publish_m5(#{client_id := ?NOT_ALLOWED_CLIENT_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_publish_m5(#{client_id := ?IGNORED_CLIENT_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_publish_m5(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{topic => <<"rewritten/topic">>}}};
auth_on_publish_m5(#{client_id := <<"modify_props">>,
                properties := #{?P_USER_PROPERTY := [#{key := <<"azE=">>,
                                                       val := <<"djE=">>},
                                                     #{key := <<"azI=">>,
                                                       val := <<"djI=">>}],
                                ?P_CORRELATION_DATA := <<"correlation_data">>,
                                ?P_RESPONSE_TOPIC := <<"responsetopic">>,
                                ?P_PAYLOAD_FORMAT_INDICATOR := <<"utf8">>,
                                ?P_CONTENT_TYPE := <<"content_type">>}}) ->
    ModifiedProps =
        #{?P_USER_PROPERTY => [#{key => <<"azE=">>, val => <<"djE=">>},
                               #{key => <<"azI=">>, val => <<"djI=">>},
                               #{key => <<"azM=">>, val => <<"djM=">>}],
          ?P_CORRELATION_DATA => <<"modified_correlation_data">>,
          ?P_RESPONSE_TOPIC => <<"modified_responsetopic">>,
          ?P_PAYLOAD_FORMAT_INDICATOR => <<"undefined">>,
          ?P_CONTENT_TYPE => <<"modified_content_type">>},
    {200, #{result => <<"ok">>,
            modifiers =>
                #{properties => ModifiedProps}}};
auth_on_publish_m5(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_subscribe(#{username := ?USERNAME,
                    client_id := ?ALLOWED_CLIENT_ID,
                    mountpoint := ?MOUNTPOINT_BIN,
                    topics := [#{topic := ?TOPIC, qos := 1}]
                 }) ->
    {200, #{result => <<"ok">>}};
auth_on_subscribe(#{client_id := ?NOT_ALLOWED_CLIENT_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_subscribe(#{client_id := ?IGNORED_CLIENT_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_subscribe(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            topics =>
                [#{topic => <<"rewritten/topic">>,
                   qos => 2},
                 #{topic => <<"forbidden/topic">>,
                   qos => 128}]}};
auth_on_subscribe(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

auth_on_subscribe_m5(#{username := ?USERNAME,
                       client_id := ?ALLOWED_CLIENT_ID,
                       mountpoint := ?MOUNTPOINT_BIN,
                       topics := [#{topic := ?TOPIC,
                                    qos:= 1,
                                    no_local := false,
                                    rap := false,
                                    retain_handling := <<"send_retain">>
                                   }],
                       properties :=
                           #{?P_USER_PROPERTY :=
                                 [#{key := <<"azE=">>,val := <<"djE=">>}],
                             ?P_SUBSCRIPTION_ID := [1,2,3]}}) ->
    {200, #{result => <<"ok">>}};
auth_on_subscribe_m5(#{client_id := ?NOT_ALLOWED_CLIENT_ID}) ->
    {200, #{result => #{error => <<"not_allowed">>}}};
auth_on_subscribe_m5(#{client_id := ?IGNORED_CLIENT_ID}) ->
    {200, #{result => <<"next">>}};
auth_on_subscribe_m5(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers =>
                #{topics =>
                      [#{topic => <<"rewritten/topic">>,
                         qos => 2,
                         no_local => false,
                         rap => false,
                         retain_handling => <<"send_retain">>},
                       #{topic => <<"forbidden/topic">>,
                         qos => 135}]}}};
auth_on_subscribe_m5(#{subscriberid := <<"internal_server_error">>}) ->
    throw(internal_server_error).

on_register(#{peer_addr := ?PEER_BIN,
              peer_port := ?PEERPORT,
              mountpoint := ?MOUNTPOINT_BIN,
              client_id := ?ALLOWED_CLIENT_ID,
              username := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_register_ok,
    {200, #{}}.

on_register_m5(#{peer_addr := ?PEER_BIN,
                 peer_port := ?PEERPORT,
                 mountpoint := ?MOUNTPOINT_BIN,
                 client_id := ?ALLOWED_CLIENT_ID,
                 username := BinPid,
                 properties :=
                     #{?P_SESSION_EXPIRY_INTERVAL := 5,
                       ?P_RECEIVE_MAX := 10,
                       ?P_TOPIC_ALIAS_MAX := 15,
                       ?P_REQUEST_RESPONSE_INFO := true,
                       ?P_REQUEST_PROBLEM_INFO := true,
                       ?P_USER_PROPERTY :=
                           [#{key := <<"azE=">>,val := <<"djE=">>}]}}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_register_m5_ok,
    {200, #{}}.

on_publish(#{username := BinPid,
             mountpoint := ?MOUNTPOINT_BIN,
             client_id := ?ALLOWED_CLIENT_ID,
             topic := ?TOPIC,
             qos := 1,
             payload := ?PAYLOAD,
             retain := false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_publish_ok,
    {200, #{}}.

on_publish_m5(#{username := BinPid,
                mountpoint := ?MOUNTPOINT_BIN,
                client_id := ?ALLOWED_CLIENT_ID,
                topic := ?TOPIC,
                qos := 1,
                payload := ?PAYLOAD,
                retain := false,
                properties := #{?P_USER_PROPERTY := [#{key := <<"azE=">>,
                                                       val := <<"djE=">>},
                                                     #{key := <<"azI=">>,
                                                       val := <<"djI=">>}],
                                ?P_CORRELATION_DATA := <<"correlation_data">>,
                                ?P_RESPONSE_TOPIC := <<"responsetopic">>,
                                ?P_PAYLOAD_FORMAT_INDICATOR := <<"utf8">>,
                                ?P_CONTENT_TYPE := <<"content_type">>}}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_publish_m5_ok,
    {200, #{}}.

on_subscribe(#{username := BinPid,
               mountpoint := ?MOUNTPOINT_BIN,
               client_id := ?ALLOWED_CLIENT_ID,
               topics := [#{topic := ?TOPIC, qos := 1},
                          #{topic := ?TOPIC, qos := 128}]
              }) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_subscribe_ok,
    {200, #{}}.

on_subscribe_m5(#{username := BinPid,
                  mountpoint := ?MOUNTPOINT_BIN,
                  client_id := ?ALLOWED_CLIENT_ID,
                  topics := [#{topic := ?TOPIC, qos := 1,
                               no_local := false,
                               rap := false,
                               retain_handling := <<"send_retain">>},
                             #{topic := ?TOPIC, qos := 128}],
                  properties :=
                      #{?P_USER_PROPERTY := [#{key := <<"azE=">>,
                                               val := <<"djE=">>}],
                        ?P_SUBSCRIPTION_ID := [1,2,3]}
                 }) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_subscribe_m5_ok,
    {200, #{}}.

on_unsubscribe(#{client_id := ?ALLOWED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>}};
on_unsubscribe(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            topics => [<<"rewritten/topic">>,
                       <<"anotherrewrittentopic">>]}}.

on_unsubscribe_m5(#{client_id := ?ALLOWED_CLIENT_ID,
                    properties := #{?P_USER_PROPERTY :=
                                        [#{key := <<"azE=">>,
                                           val := <<"djE=">>}]}}) ->
    {200, #{result => <<"ok">>}};
on_unsubscribe_m5(#{client_id := ?CHANGED_CLIENT_ID}) ->
    {200, #{result => <<"ok">>,
            modifiers => #{topics => [<<"rewritten/topic">>]}}}.

on_deliver(#{username := BinPid,
             mountpoint := ?MOUNTPOINT_BIN,
             client_id := ?ALLOWED_CLIENT_ID,
             qos := 1,
             topic := ?TOPIC,
             payload := ?PAYLOAD,
             retain := false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_deliver_ok,
    {200, #{result => <<"ok">>}}.

on_deliver_m5(#{username := BinPid,
                mountpoint := ?MOUNTPOINT_BIN,
                client_id := ?ALLOWED_CLIENT_ID,
                qos := 1,
                topic := ?TOPIC,
                payload := ?PAYLOAD,
                retain := false}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_deliver_m5_ok,
    {200, #{result => <<"ok">>}};
on_deliver_m5(#{client_id := <<"modify_props">>,
                username := BinPid,
                properties := #{?P_USER_PROPERTY := [#{key := <<"azE=">>,
                                                       val := <<"djE=">>},
                                                     #{key := <<"azI=">>,
                                                       val := <<"djI=">>}],
                                ?P_CORRELATION_DATA := <<"correlation_data">>,
                                ?P_RESPONSE_TOPIC := <<"responsetopic">>,
                                ?P_PAYLOAD_FORMAT_INDICATOR := <<"utf8">>,
                                ?P_CONTENT_TYPE := <<"content_type">>}}) ->
    ModifiedProps =
        #{?P_USER_PROPERTY => [#{key => <<"azE=">>, val => <<"djE=">>},
                               #{key => <<"azI=">>, val => <<"djI=">>},
                               #{key => <<"azM=">>, val => <<"djM=">>}],
          ?P_CORRELATION_DATA => <<"modified_correlation_data">>,
          ?P_RESPONSE_TOPIC => <<"modified_responsetopic">>,
          ?P_PAYLOAD_FORMAT_INDICATOR => <<"undefined">>,
          ?P_CONTENT_TYPE => <<"modified_content_type">>},
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_deliver_m5_ok,
    {200, #{result => <<"ok">>,
            modifiers =>
                #{properties => ModifiedProps}}}.

on_offline_message(#{mountpoint := ?MOUNTPOINT_BIN,
                     client_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_offline_message_ok,
    {200, #{}}.

on_client_wakeup(#{mountpoint := ?MOUNTPOINT_BIN,
                   client_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_wakeup_ok,
    {200, #{}}.

on_client_offline(#{mountpoint := ?MOUNTPOINT_BIN,
                   client_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_offline_ok,
    {200, #{}}.

on_client_gone(#{mountpoint := ?MOUNTPOINT_BIN,
                 client_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_client_gone_ok,
    {200, #{}}.

on_session_expired(#{mountpoint := ?MOUNTPOINT_BIN,
                     client_id := BinPid}) ->
    Pid = list_to_pid(binary_to_list(BinPid)),
    Pid ! on_session_expired_ok,
    {200, #{}}.

on_auth_m5(#{properties :=
                 #{?P_AUTHENTICATION_METHOD := <<"AUTH_METHOD">>,
                   ?P_AUTHENTICATION_DATA := <<"QVVUSF9EQVRBMA==">>}, %% b64(<<"AUTH_DATA0">>)
             username := ?USERNAME,
             mountpoint := ?MOUNTPOINT_BIN,
             client_id := ?ALLOWED_CLIENT_ID}) ->
    Props = #{?P_AUTHENTICATION_METHOD => <<"AUTH_METHOD">>,
              ?P_AUTHENTICATION_DATA => base64:encode(<<"AUTH_DATA1">>)},
    {200, #{result => <<"ok">>,
            modifiers =>
                #{properties => Props,
                  reason_code => 0}}}.

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
    on_client_gone(Body);
process_hook(<<"on_session_expired">>, Body) ->
    on_session_expired(Body);

process_hook(<<"auth_on_register_m5">>, Body) ->
    auth_on_register_m5(Body);
process_hook(<<"auth_on_publish_m5">>, Body) ->
    auth_on_publish_m5(Body);
process_hook(<<"auth_on_subscribe_m5">>, Body) ->
    auth_on_subscribe_m5(Body);
process_hook(<<"on_register_m5">>, Body) ->
    on_register_m5(Body);
process_hook(<<"on_publish_m5">>, Body) ->
    on_publish_m5(Body);
process_hook(<<"on_subscribe_m5">>, Body) ->
    on_subscribe_m5(Body);
process_hook(<<"on_unsubscribe_m5">>, Body) ->
    on_unsubscribe_m5(Body);
process_hook(<<"on_deliver_m5">>, Body) ->
    on_deliver_m5(Body);
process_hook(<<"on_auth_m5">>, Body) ->
    on_auth_m5(Body).

