-module(vmq_mqtt5_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").

init_per_suite(Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:listener_start(1888, []),
    cover:start(),
    Config.

end_per_suite(_Config) ->
    vmq_test_utils:teardown(),
    ok.

init_per_testcase(_TestCase, Config) ->
    vmq_server_cmd:set_config(allow_anonymous, false),
    vmq_server_cmd:set_config(max_client_id_size, 23),
    vmq_config:configure_node(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

all() ->
    [
     {group, mqtt}%% ,
     %% {group, mqtts}
    ].

groups() ->
    Tests = 
    [anon_success,
     empty_client_id,
     invalid_id%% ,
     %% session_take_over,
     %% uname_no_password_success,
     %% password_no_uname_success,
     %% uname_password_denied,
     %% uname_password_success,
     %% change_subscriber_id
    ],
    [
     {mqtt, [], Tests}
     %%{mqtts, [], Tests}
    ].

anon_success(_Config) ->
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_config:configure_node(),
    Connect = vmq_parser_mqtt5:gen_connect("connect-success-test", [{keepalive,10}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_CONNACK_ACCEPT,
                   properties = []}
        = Connack,
    ok = gen_tcp:close(Socket).

empty_client_id(_Config) ->
    %% A Server MAY allow a Client to supply a ClientID that has a
    %% length of zero bytes, however if it does so the Server MUST
    %% treat this as a special case and assign a unique ClientID to
    %% that Client [MQTT-3.1.3-6]. It MUST then process the CONNECT
    %% packet as if the Client had provided that unique ClientID, and
    %% MUST return the Assigned Client Identifier in the CONNACK
    %% packet [MQTT-3.1.3-7].
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_config:configure_node(),
    CSTrue = vmq_parser_mqtt5:gen_connect(empty, [{keepalive,10}, {clean_start, true}]),
    {ok, Socket0, Connack0, <<>>} = packetv5:do_client_connect(CSTrue, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_CONNACK_ACCEPT,
                   properties = [#p_assigned_client_id{value = _}]}
        = Connack0,
    ok = gen_tcp:close(Socket0),
    
    CSFalse = vmq_parser_mqtt5:gen_connect(empty, [{keepalive,10}, {clean_start, false}]),
    {ok, Socket1, Connack1, <<>>} = packetv5:do_client_connect(CSFalse, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_CONNACK_ACCEPT,
                   properties = [#p_assigned_client_id{value = _}]}
        = Connack1,
    ok = gen_tcp:close(Socket1).

invalid_id(_Config) ->
    %% If the Server rejects the ClientID it MAY respond to the
    %% CONNECT packet with a CONNACK using Reason Code 0x85 (Client
    %% Identifier not valid) as described in section 4.13 Handling
    %% errors, and then itMUST close the Network Connection
    %% [MQTT-3.1.3-8].
    Id = <<"this client id is longer than the set limit of 23 bytes and thus invalid">>,
    Connect = vmq_parser_mqtt5:gen_connect(Id, [{keepalive,10}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_CLIENT_IDENTIFIER_NOT_VALID,
                   properties = []}
        = Connack,
    ok = gen_tcp:close(Socket).
    
session_take_over(_Config) ->
    %% If the ClientID represents a Client already connected to the
    %% Server, the Server send a DISCONNECT packet to the existing
    %% Client with Reason Code of 0x8E (Session taken over) as
    %% described in section 4.13 and MUST close the Network Connection
    %% of the existing Client [MQTT-3.1.4-3].
    vmq_server_cmd:set_config(allow_anonymous, true),
    vmq_server_cmd:set_config(max_client_id_size, 50),
    vmq_config:configure_node(),
    Connect = vmq_parser_mqtt5:gen_connect("connect-session-takeover", [{keepalive,10}]),
    {ok, Socket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_CONNACK_ACCEPT,
                   properties = []}
        = Connack,
    {ok, NewSocket, Connack, <<>>} = packetv5:do_client_connect(Connect, []),
    #mqtt5_connack{session_present = 0,
                   reason_code = ?M5_CONNACK_ACCEPT,
                   properties = []}
        = Connack,
    {ok, Socket, Disconnect, <<>>} = packetv5:receive_frame(Socket),
    #mqtt5_disconnect{reason_code = ?M5_SESSION_TAKEN_OVER,
                      properties = []}
        = Disconnect,
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(NewSocket).

uname_no_password_success(_Config) ->
    %% Connect = packet:gen_connect("connect-uname-test-", [{keepalive,10}, {username, "user"}]),
    %% Connack = packet:gen_connack(4),
    %% ok = vmq_plugin_mgr:enable_module_plugin(
    %%   auth_on_register, ?MODULE, hook_uname_no_password_denied, 5),
    %% {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% ok = vmq_plugin_mgr:disable_module_plugin(
    %%   auth_on_register, ?MODULE, hook_uname_no_password_denied, 5),
    %% ok = gen_tcp:close(Socket).
    throw(not_implemented).

password_no_uname_success(_Config) ->
    throw(not_implemented).

uname_password_denied(_Config) ->
    throw(not_implemented).

uname_password_success(_Config) ->
    throw(not_implemented).

change_subscriber_id(_Config) ->
    %% TODO: should we be allowed to do this? If we should, should we
    %% then respond with the assigned_client_id property?
    throw(not_implemented).


    
