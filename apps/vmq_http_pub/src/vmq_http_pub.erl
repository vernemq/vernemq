%% Copyright 2023-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_http_pub).
-include_lib("kernel/include/logger.hrl").
-behaviour(vmq_http_config).

%% cowboy rest handler callbacks
-export([
    init/2,
    allowed_methods/2,
    content_types_accepted/2,
    is_authorized/2,
    process_post/2,
    start/0,
    stop/0
]).

-ifdef(TEST).
-export([
    auth_conf/1,
    config_ovr_listener/10
]).
-endif.

-record(pubrec, {topic, qos, retain, mountpoint}).
-record(authrec, {user, password, clientid}).

%%====================================================================
%% API functions
%%====================================================================

-export([
    routes/0
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Plugin Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    {ok, _} = application:ensure_all_started(vmq_http_pub),
    ok.

stop() ->
    application:stop(vmq_http_pub).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy REST Handler (GENERIC)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"POST">>], Req, State}.

content_types_accepted(Req, State) ->
    %% Return the content types that this resource can accept in a request
    {
        [{<<"application/json">>, process_post}, {<<"application/octet-stream">>, process_post}],
        Req,
        State
    }.

%%
%% Publish
%%

publish(
    State,
    #pubrec{topic = Topic, qos = QoS, retain = Retain, mountpoint = Mountpoint} = _PubRec,
    Payload,
    UserProperties
) ->
    Publish = proplists:get_value(publish_fun, State),
    TopicWord = vmq_topic:word(binary_to_list(Topic)),
    Publish(TopicWord, Payload, #{
        qos => QoS, retain => Retain, mountpoint => Mountpoint, user_property => UserProperties
    }),
    _ = incr_mqtt_publish_sent(),
    {ok, ok_published, [{topic, TopicWord}, {qos, QoS}, {retain, Retain}, {mp, Mountpoint}]}.

maybe_publish_s2(State, PubRec, Payload, UserProperties) ->
    case check_values(PubRec, UserProperties) of
        {ok} -> publish(State, PubRec, Payload, UserProperties);
        Error -> Error
    end.

config_ovr_listener(
    Config_lvl1_atom,
    Config_lvl1_sub_atom,
    Config_lvl1_item,
    Default,
    ListenerProtocol,
    ListenerName,
    Config_lvl2_atom,
    Config_lvl2_sub_atom,
    Config_lvl2_item1,
    Config_lvl2_item2
) ->
    Config = application:get_env(Config_lvl1_atom, Config_lvl1_sub_atom, []),
    Level1 = proplists:get_value(Config_lvl1_item, Config, Default),

    Level2 = maps:get(
        case Config_lvl2_item2 of
            undefined -> {ListenerProtocol, ListenerName, Config_lvl2_item1};
            _ -> {ListenerProtocol, ListenerName, Config_lvl2_item1, Config_lvl2_item2}
        end,
        application:get_env(Config_lvl2_atom, Config_lvl2_sub_atom, #{}),
        undefined
    ),

    case {Level1, Level2} of
        {undefined, undefined} -> undefined;
        {_, L2} when length(L2) > 3 -> {l2, L2};
        {L1, _} -> {l1, L1}
    end.

listener_name_and_protocol(Req) ->
    Reg = maps:get(ref, Req),
    Protocol = binary_to_atom(maps:get(scheme, Req)),
    Opts = ranch:get_protocol_options(Reg),
    ListenerName =
        case lists:keyfind(listener_name, 1, maps:get(opts, Opts)) of
            false -> undefined;
            Tuple -> list_to_atom(element(2, Tuple))
        end,
    {Protocol, ListenerName}.

mountpoint(Req, Mountpoint) ->
    {Protocol, ListenerName} = listener_name_and_protocol(Req),
    case
        config_ovr_listener(
            vmq_http_pub,
            config,
            mqttmountpoint,
            Mountpoint,
            Protocol,
            ListenerName,
            vmq_http_pub,
            listener_config,
            mountpoint,
            undefined
        )
    of
        undefined -> "";
        {_, Setting} -> Setting
    end.

auth_conf(Req) ->
    {Protocol, ListenerName} = listener_name_and_protocol(Req),

    {_, Auth} = config_ovr_listener(
        vmq_http_pub,
        config,
        mqttauth,
        [],
        Protocol,
        ListenerName,
        vmq_http_pub,
        listener_config,
        mqtt_auth,
        mode
    ),

    case Auth of
        "on-behalf-of" ->
            {ok, Auth, []};
        "predefined" ->
            {_, Mqttauthuser} = config_ovr_listener(
                vmq_http_pub,
                config,
                mqttauthuser,
                [],
                Protocol,
                ListenerName,
                vmq_http_pub,
                listener_config,
                mqtt_auth,
                user
            ),
            {_, Mqttauthpassword} = config_ovr_listener(
                vmq_http_pub,
                config,
                mqttauthpassword,
                [],
                Protocol,
                ListenerName,
                vmq_http_pub,
                listener_config,
                mqtt_auth,
                password
            ),
            {_, Mqttauthclientid} = config_ovr_listener(
                vmq_http_pub,
                config,
                mqttauthclientid,
                [],
                Protocol,
                ListenerName,
                vmq_http_pub,
                listener_config,
                mqtt_auth,
                client_id
            ),

            {ok, Auth, [
                {mqttauthuser, Mqttauthuser},
                {mqttauthpassword, Mqttauthpassword},
                {mqttauthclientid, Mqttauthclientid}
            ]};
        "testMode" ->
            {ok, Auth, []}
    end.

check_overrides([], [], _, Mountpoint, Topic, QoS, MaxMessageSize) ->
    {ok, Mountpoint, Topic, QoS, MaxMessageSize};
check_overrides(OvrAuthRegister, OvrAuthPublish, ClientId, Mountpoint, Topic, QoS, MaxMessageSize) ->
    COvrMaxMessageSize = proplists:get_value(max_message_size, OvrAuthRegister, MaxMessageSize),

    %% Apply overrides from authentication, we support only Mountpoint, Topic and QoS
    {ok, CIntMountpoint} = check_subscriber_ovr(
        proplists:get_value(subscriber_id, OvrAuthRegister, {Mountpoint, ClientId}), ClientId
    ),
    COvrMountpoint = proplists:get_value(mountpoint, OvrAuthPublish, CIntMountpoint),

    % Topic and QoS are only supported on on_publish
    CTopic = proplists:get_value(topic, OvrAuthPublish, Topic),
    COvrTopic =
        case CTopic of
            [<<>>] -> <<>>;
            X when is_list(X) -> iolist_to_binary(vmq_topic:unword(CTopic));
            X -> X
        end,
    COvrQoS = proplists:get_value(qos, OvrAuthPublish, QoS),
    {ok, COvrMountpoint, COvrTopic, COvrQoS, COvrMaxMessageSize}.

check_message_size(Payload, MaxMessageSize) ->
    Ret = (MaxMessageSize =< 1) orelse (byte_size(Payload) =< MaxMessageSize),
    Ret.

maybe_publish(
    Req,
    State,
    AuthRec,
    #pubrec{topic = Topic, retain = Retain, mountpoint = Mountpoint, qos = QoS},
    Payload,
    UserProperties,
    CheckParameterEncoding
) ->
    {ok, Mode, Config} = auth_conf(Req),
    AuthRec0 =
        case Mode of
            "predefined" ->
                #authrec{
                    user = list_to_binary(proplists:get_value(mqttauthuser, Config, undefined)),
                    password = list_to_binary(
                        proplists:get_value(mqttauthpassword, Config, undefined)
                    ),
                    clientid = list_to_binary(
                        proplists:get_value(mqttauthclientid, Config, undefined)
                    )
                };
            _ ->
                AuthRec
        end,

    CMountpoint = mountpoint(Req, Mountpoint),

    IQoS = vmq_util:to_int(QoS, -99),
    BRetain = vmq_util:to_bool(Retain, invalid),

    Peer = maps:get(peer, Req),

    CA = check_auth(Peer, AuthRec0, Topic, IQoS, CMountpoint, Payload),

    case CA of
        {ok, Overrides1, Overrides2} ->
            MaxMessageSize = vmq_config:get_env(max_message_size, 0),
            {ok, COvrMountpoint, COvrTopic, COvrQoS, COvrMaxMessageSize} = check_overrides(
                Overrides1,
                Overrides2,
                AuthRec0#authrec.clientid,
                CMountpoint,
                Topic,
                IQoS,
                MaxMessageSize
            ),
            case check_message_size(Payload, COvrMaxMessageSize) of
                true ->
                    case check_payload_encoding(Req, CheckParameterEncoding) of
                        {ok, plain} ->
                            maybe_publish_s2(
                                State,
                                #pubrec{
                                    topic = COvrTopic,
                                    qos = COvrQoS,
                                    retain = BRetain,
                                    mountpoint = COvrMountpoint
                                },
                                Payload,
                                UserProperties
                            );
                        {ok, base64} ->
                            PayloadDecoded = base64:decode(Payload),
                            maybe_publish_s2(
                                State,
                                #pubrec{
                                    topic = COvrTopic,
                                    qos = COvrQoS,
                                    retain = BRetain,
                                    mountpoint = COvrMountpoint
                                },
                                PayloadDecoded,
                                UserProperties
                            );
                        {error, _} ->
                            {error, unknown_payload_encoding, []}
                    end;
                _ ->
                    {error, message_too_large, []}
            end;
        {error, authorization_failed, EData} ->
            {error, authorization_failed, EData};
        _ ->
            {error, authentication_failed, []}
    end.

check_payload_encoding(Req, true) ->
    Params = cowboy_req:parse_qs(Req),
    case proplists:get_value(<<"encoding">>, Params, undefined) of
        undefined ->
            {ok, plain};
        Value when Value =:= <<"plain">> ->
            {ok, plain};
        Value when Value =:= <<"base64">> ->
            {ok, base64};
        _ ->
            {error, undefined_encoding_specified}
    end;
check_payload_encoding(_, _) ->
    {ok, plain}.

check_encoding_flag(Req) ->
    Params = cowboy_req:parse_qs(Req),
    case proplists:get_value(<<"encoding">>, Params, undefined) of
        undefined ->
            true;
        Value when Value =:= <<"plain">>; Value =:= <<"base64">> ->
            % no other URL-encoded parameter exists
            length(Params) =:= 1;
        _ ->
            false
    end.

list_from_map(undefined) -> [];
list_from_map({error, Error}) -> {error, Error};
list_from_map(ListOfMap) -> maps:to_list(lists:nth(1, ListOfMap)).

parse_request_json(Req, State, Body) ->
    JsonData = vmq_json:decode(Body),

    %% Used for authentication (optional, depending on the authentication)
    ClientIdJson = maps:get(<<"client_id">>, JsonData, undefined),
    UserJson = maps:get(<<"user">>, JsonData, undefined),
    PasswordJSon = maps:get(<<"password">>, JsonData, undefined),
    {UserConfig, PasswordConfig, ClientIdConfig, Mountpoint} = read_from_config(Req),

    AuthRec = #authrec{
        clientid = vmq_util:nvl(ClientIdConfig, ClientIdJson),
        user = vmq_util:nvl(UserConfig, UserJson),
        password = vmq_util:nvl(PasswordConfig, PasswordJSon)
    },

    PubRec = #pubrec{
        topic = maps:get(<<"topic">>, JsonData),
        qos = maps:get(<<"qos">>, JsonData, 0),
        retain = maps:get(<<"retain">>, JsonData, false),
        mountpoint = Mountpoint
    },
    Payload = maps:get(<<"payload">>, JsonData),
    UserProperties = list_from_map(maps:get(<<"user_properties">>, JsonData, undefined)),
    maybe_publish(
        Req,
        State,
        AuthRec,
        PubRec,
        Payload,
        UserProperties,
        true
    ).

process_post_json_body(Req, State) ->
    {ok, Body, _} = cowboy_req:read_body(Req),
    %% Process the request body and return a response
    case vmq_json:is_json(Body) of
        true -> parse_request_json(Req, State, Body);
        _ -> {error, content_not_json, []}
    end.

%% todo: a non json UserProperty will raise an internal server error. It is an error but, could be made nicer
user_properties_json_parse(undefined) ->
    undefined;
user_properties_json_parse(UserProperties) ->
    try
        Json = vmq_json:decode(UserProperties),
        Json
    catch
        error:badarg -> {error, invalid_user_properties}
    end.

process_post_header(Req, State) ->
    %% Used for authentication (optional, depending on the authentication)
    ClientIdHeader = cowboy_req:header(<<"client_id">>, Req, undefined),
    UserHeader = cowboy_req:header(<<"user">>, Req, undefined),
    PasswordHeader = cowboy_req:header(<<"password">>, Req, undefined),
    {UserConfig, PasswordConfig, ClientIdConfig, Mountpoint} = read_from_config(Req),

    AuthRec = #authrec{
        clientid = vmq_util:nvl(ClientIdConfig, ClientIdHeader),
        user = vmq_util:nvl(UserConfig, UserHeader),
        password = vmq_util:nvl(PasswordConfig, PasswordHeader)
    },

    PubRec = #pubrec{
        topic = cowboy_req:header(<<"topic">>, Req),
        qos = cowboy_req:header(<<"qos">>, Req, 0),
        retain = cowboy_req:header(<<"retain">>, Req, <<"false">>),
        mountpoint = Mountpoint
    },
    UserPropertiesRaw = cowboy_req:header(<<"user_properties">>, Req, undefined),
    UserProperties = list_from_map(user_properties_json_parse(UserPropertiesRaw)),
    {ok, Payload, _} = cowboy_req:read_body(Req),
    maybe_publish(
        Req,
        State,
        AuthRec,
        PubRec,
        Payload,
        UserProperties,
        false
    ).

process_post_urlencoded(Req, State) ->
    Params = cowboy_req:parse_qs(Req),

    %% Used for authentication (optional, depending on the authentication)
    ClientIdQS = proplists:get_value(<<"client_id">>, Params, undefined),
    UserQS = proplists:get_value(<<"user">>, Params, undefined),
    PasswordQS = proplists:get_value(<<"password">>, Params, undefined),
    {UserConfig, PasswordConfig, ClientIdConfig, Mountpoint} = read_from_config(Req),
    AuthRec = #authrec{
        clientid = vmq_util:nvl(ClientIdConfig, ClientIdQS),
        user = vmq_util:nvl(UserConfig, UserQS),
        password = vmq_util:nvl(PasswordConfig, PasswordQS)
    },

    PubRec = #pubrec{
        topic = proplists:get_value(<<"topic">>, Params, undefined),
        qos = proplists:get_value(<<"qos">>, Params, 0),
        retain = proplists:get_value(<<"retain">>, Params, false),
        mountpoint = Mountpoint
    },
    UserProperties = [],
    {ok, Payload, _} = cowboy_req:read_body(Req),
    maybe_publish(
        Req,
        State,
        AuthRec,
        PubRec,
        Payload,
        UserProperties,
        false
    ).

%%
%% Main entry for post
%%
process_post(Req, State) ->
    {_, Resp, Data} =
        case {is_post_header(Req), is_post_urlencoded(Req), check_encoding_flag(Req)} of
            {true, false, _} -> process_post_header(Req, State);
            {false, true, _} -> process_post_urlencoded(Req, State);
            {true, true, false} -> {error, parameters_in_header_and_url, []};
            {_, _, false} -> {error, wrong_url_parameters, []};
            {false, false, true} -> process_post_json_body(Req, State)
        end,
    case map_to_response_code(Resp) of
        400 ->
            incr_mqtt_error();
        401 ->
            ?LOG_ERROR("Authentication failure."),
            incr_mqtt_auth_error();
        403 ->
            ?LOG_ERROR("Authorization failure."),
            incr_mqtt_auth_error();
        _ ->
            ok
    end,
    ResponseText = map_to_response_text(Resp),
    DataText = list_to_binary(data_to_text(Data)),

    Req2 = cowboy_req:reply(
        map_to_response_code(Resp),
        #{<<"content-type">> => <<"text/plain">>},
        <<ResponseText/binary, DataText/binary>>,
        Req
    ),
    {stop, Req2, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Authentication / Authorization (ENDPOINT)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, vmq_http_pub),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "httppub");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Authentication / Authorization (User / Client)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Currently we support only authentication via "on_behalf_of", or by having a predefined
% user. Furthermore, anonymous access is supported. We can add further authentication schemes later.

check_subscriber_ovr(SubscriberOvr, ClientId) ->
    NewMP = element(1, SubscriberOvr),
    NewClientId = element(2, SubscriberOvr),
    case NewClientId of
        ClientId -> {ok, NewMP};
        _ -> {error, cannot_override_clientid, []}
    end.

-dialyzer({nowarn_function, check_auth/6}).
check_auth(
    Peer,
    #authrec{user = User, password = Password, clientid = ClientId},
    Topic,
    QoS,
    Mountpoint,
    Payload
) ->
    AllowAnonymous = vmq_config:get_env(allow_anonymous, false),
    case AllowAnonymous of
        false ->
            AuthOnRegister =
                case auth_on_register(Peer, {Mountpoint, ClientId}, User, Password) of
                    {ok} -> {ok, []};
                    {ok, Overrides} -> {ok, Overrides};
                    {error, _} -> {error, authentication_failed, []}
                end,

            {ok, COvrMountpoint} =
                case AuthOnRegister of
                    {ok, []} ->
                        {ok, Mountpoint};
                    {ok, Ovr} ->
                        {ok, MPPOvr, _COvrTopic, _COvrQoS, _CMessageSize} = check_overrides(
                            Ovr, [], ClientId, Mountpoint, Topic, QoS, 0
                        ),
                        {ok, MPPOvr};
                    _ ->
                        {ok, Mountpoint}
                end,

            AuthOnPublish =
                case AuthOnRegister of
                    {ok, _} ->
                        case
                            auth_on_publish(
                                {COvrMountpoint, ClientId}, User, Topic, QoS, Payload
                            )
                        of
                            {ok} -> {ok, []};
                            {ok, OverridesOnPublish} -> {ok, OverridesOnPublish};
                            {error, _} -> {error, authorization_failed, []}
                        end;
                    _ ->
                        {error, authentication_failed, []}
                end,
            case {AuthOnRegister, AuthOnPublish} of
                {{ok, []}, {ok, []}} ->
                    {ok, [], []};
                {{ok, OverridesRegister}, {ok, OverridesPublish}} ->
                    {ok, OverridesRegister, OverridesPublish};
                {{error, Error, ErrorData}, _} ->
                    {error, Error, ErrorData};
                {_, {error, Error, ErrorData}} ->
                    {error, Error, ErrorData}
            end;
        _ ->
            {ok, [], []}
    end.

auth_on_register(Peer, SubscriberId, User, Password) ->
    HookArgs = [Peer, SubscriberId, User, Password, true],
    case vmq_plugin:all_till_ok(auth_on_register, HookArgs) of
        ok ->
            {ok};
        {ok, Args} ->
            {ok, Args};
        {error, Reason} ->
            ?LOG_ERROR("can't auth register ~p due to ~p", [HookArgs, Reason]),
            {error, Reason}
    end.

auth_on_publish(SubscriberId, User, Topic, QoS, Payload) ->
    HookArgs = [User, SubscriberId, QoS, vmq_topic:word(binary_to_list(Topic)), Payload, false],
    case vmq_plugin:all_till_ok(auth_on_publish, HookArgs) of
        ok ->
            {ok};
        {ok, Args} when is_list(Args) ->
            {ok, Args};
        {ok, ChangedPayload} when is_binary(ChangedPayload) ->
            % we do not support changed payload
            ?LOG_ERROR("ChangePayload is not supported."),
            {error, not_allowed};
        {error, Reason} ->
            ?LOG_ERROR("can't auth publish ~p due to ~p", [HookArgs, Reason]),
            {error, not_allowed}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Configuration check
%% The REST Bridge can be called in three different ways:
%% Header       => All necessary attributes (Topic, Payload...) need to be delivered as part of the header,
%%                 the payload is in the body and will be published without modification as an MQTT topic.
%% URL Encoded  => All parameters are URL encoded
%% JSON payload => Parameters and payload are part of JSON
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

is_post_header(Req) ->
    case cowboy_req:header(<<"topic">>, Req) of
        undefined ->
            false;
        _ ->
            true
    end.

is_post_urlencoded(Req) ->
    case lists:keyfind(<<"topic">>, 1, cowboy_req:parse_qs(Req)) of
        false -> false;
        _ -> true
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Input check
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

is_key_value_list([]) ->
    true;
is_key_value_list([{_, _} | Rest]) ->
    is_key_value_list(Rest);
is_key_value_list(_) ->
    false.

is_valid_mqtt_topic(Topic) ->
    case Topic of
        undefined ->
            false;
        [] ->
            false;
        _ ->
            case vmq_topic:validate_topic(publish, Topic) of
                {ok, _} -> binary:longest_common_prefix([<<"$">>, Topic]) == 0;
                _ -> false
            end
    end.

check_values(#pubrec{topic = Topic, qos = QoS, retain = Retain}, UserProperties) ->
    %% QOS 0,1,2
    case is_valid_mqtt_topic(Topic) of
        true ->
            case QoS of
                V when is_integer(V), V >= 0, V < 3 ->
                    case is_boolean(Retain) of
                        true ->
                            case is_key_value_list(UserProperties) of
                                true -> {ok};
                                false -> {error, invalid_user_properties, []}
                            end;
                        false ->
                            {error, invalid_retain_value, []}
                    end;
                _ ->
                    {error, invalid_qos_value, []}
            end;
        _ ->
            {error, invalid_topic, []}
    end.

-ifdef(TEST).
data_to_text(Data) ->
    io_lib:format("~p", [Data]).
-endif.
-ifndef(TEST).
data_to_text(_) ->
    "".
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Configuration parameters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_from_config(_) ->
    {undefined, undefined, undefined, undefined}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% METRICS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
incr_mqtt_publish_sent() ->
    vmq_http_pub_sup:incr_metric(published, 1).

incr_mqtt_error() ->
    vmq_http_pub_sup:incr_metric(api_error, 1).

incr_mqtt_auth_error() ->
    vmq_http_pub_sup:incr_metric(auth_error, 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RESPONSE TEXTS AND CODES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_to_response_text(invalid_topic) ->
    <<"Invalid topic">>;
map_to_response_text(invalid_qos_value) ->
    <<"Invalid QoS value. Must be in range 0..2">>;
map_to_response_text(invalid_retain_value) ->
    <<"Invalid retain flag. Must be in true or false">>;
map_to_response_text(invalid_user_properties) ->
    <<"User properties need to be a key/value list (json encoded)">>;
map_to_response_text(ok_published) ->
    <<"Published">>;
map_to_response_text(parameters_in_header_and_url) ->
    <<"Parameters found in header as well as url-encoded">>;
map_to_response_text(content_not_json) ->
    <<"Content is not a valid json">>;
map_to_response_text(wrong_url_parameters) ->
    <<"Only encoding plain or base64 allows. No more URL parameters supported.">>;
map_to_response_text(authentication_failed) ->
    <<"Authentication failure.">>;
map_to_response_text(authorization_failed) ->
    <<"Authorization failure.">>;
map_to_response_text(message_too_large) ->
    <<"Payload too large.">>;
map_to_response_text(unknown_payload_encoding) ->
    <<"Unknown payload encoding">>.

map_to_response_code(invalid_topic) -> 400;
map_to_response_code(invalid_qos_value) -> 400;
map_to_response_code(invalid_retain_value) -> 400;
map_to_response_code(invalid_user_properties) -> 400;
map_to_response_code(ok_published) -> 200;
map_to_response_code(parameters_in_header_and_url) -> 400;
map_to_response_code(content_not_json) -> 400;
map_to_response_code(wrong_url_parameters) -> 400;
map_to_response_code(authentication_failed) -> 401;
map_to_response_code(authorization_failed) -> 403;
map_to_response_code(message_too_large) -> 413;
map_to_response_code(unknown_payload_encoding) -> 400.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
routes() ->
    {ok, #{
        publish_fun := PubFun
    }} = vmq_reg:direct_plugin_exports(vmq_http_pub, #{
        wait_till_ready => not vmq_config:get_env(allow_publish_during_netsplit, false),
        cap_publish => vmq_config:get_env(allow_publish_during_netsplit, false)
    }),
    [{"/restmqtt/api/v1/publish", ?MODULE, [{publish_fun, PubFun}]}].
