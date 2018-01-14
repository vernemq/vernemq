-module(vmq_parser_mqtt5_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("vmq_commons/include/vmq_types_mqtt5.hrl").

-import(vmq_parser_mqtt5, [enc_properties/1,
                           parse_properties/2]).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() -> 
    [parse_unparse_tests,
     parse_unparse_properties_test].


parse_unparse_tests(_Config) ->
    Properties = {properties, [#p_session_expiry_interval{value = 12341234}]},
    parse_unparse("connect", vmq_parser_mqtt5:gen_connect("test-client", [Properties])).


parse_unparse_properties_test(_Config) ->
    parse_unparse_property(#p_payload_format_indicator{value = utf8}),
    parse_unparse_property(#p_payload_format_indicator{value = unspecified}),

    parse_unparse_property(#p_message_expiry_interval{value = 123}),

    parse_unparse_property(#p_content_type{value = <<"some content type">>}),

    parse_unparse_property(#p_response_topic{value = <<"a response topic">>}),

    parse_unparse_property(#p_correlation_data{value = <<"correlation data">>}),

    parse_unparse_property(#p_subscription_id{value = 123412345}),

    parse_unparse_property(#p_session_expiry_interval{value = 123412345}),

    parse_unparse_property(#p_assigned_client_id{value = <<"assigned client id">>}),

    parse_unparse_property(#p_server_keep_alive{value = 3600}),

    parse_unparse_property(#p_authentication_method{value = <<"authentication method">>}),

    parse_unparse_property(#p_authentication_data{value = <<"authentication data">>}),

    parse_unparse_property(#p_request_problem_info{value = true}),
    parse_unparse_property(#p_request_problem_info{value = false}),

    parse_unparse_property(#p_will_delay_interval{value = 3600}),

    parse_unparse_property(#p_request_response_info{value = true}),
    parse_unparse_property(#p_request_response_info{value = false}),

    parse_unparse_property(#p_server_ref{value = <<"server reference">>}),

    parse_unparse_property(#p_reason_string{value = <<"reason string">>}),

    parse_unparse_property(#p_receive_max{value = 65535}),

    parse_unparse_property(#p_topic_alias_max{value = 65535}),

    parse_unparse_property(#p_topic_alias{value = 65535}),

    parse_unparse_property(#p_max_qos{value = 0}),
    parse_unparse_property(#p_max_qos{value = 1}),

    parse_unparse_property(#p_retain_available{value = true}),
    parse_unparse_property(#p_retain_available{value = false}),

    parse_unparse_property(#p_user_property{value = {<<"key">>, <<"val">>}}),

    parse_unparse_property(#p_max_packet_size{value = 12341234}),

    parse_unparse_property(#p_wildcard_subs_available{value = true}),
    parse_unparse_property(#p_wildcard_subs_available{value = false}),

    parse_unparse_property(#p_sub_ids_available{value = true}),
    parse_unparse_property(#p_sub_ids_available{value = false}),

    parse_unparse_property(#p_shared_subs_available{value = true}),
    parse_unparse_property(#p_shared_subs_available{value = false}).

parse_unparse_property(Property) ->
    Encoded = enc_property_([Property]),
    [Parsed] = parse_properties(Encoded, []),
    compare_property(Property, Parsed).

enc_property_(Property) ->
    iolist_to_binary(enc_properties(Property)).

compare_property(P, P) -> true.

parse_unparse(Test, Frame) ->
    io:format(user, "parse/unparse: ~p~n", [Test]),
    {ParsedFrame, <<>>} = vmq_parser_mqtt5:parse(Frame),
    SerializedFrame = iolist_to_binary(vmq_parser_mqtt5:serialise(ParsedFrame)),
    compare_frame(Test, Frame, SerializedFrame).

compare_frame(_, F, F) -> true.
