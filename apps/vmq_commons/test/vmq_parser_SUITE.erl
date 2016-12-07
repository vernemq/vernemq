-module(vmq_parser_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

all() ->
    [more_data_test,
     parse_unparse_tests].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

more_data_test(_Config) ->
    CompletePacket = packet:gen_publish(<<"a nice topic">>, 1, <<"a complete payload">>, []),
    PartialSize = byte_size(CompletePacket) - 1,
    <<IncompletePacket:PartialSize/binary, _LastByte/binary>> = CompletePacket,
    more = vmq_parser:parse(IncompletePacket).

parse_unparse_tests(_Config) ->
    compare_frame("connect1", vmq_parser:gen_connect("test-client", [])),
    compare_frame("connect2", vmq_parser:gen_connect("test-client", [{will_topic, "test-will-topic"},
                                                          {will_msg, "this is a samp"},
                                                          {will_qos, 2},
                                                          {username, "joe"},
                                                          {password, "secret"}])),
    compare_frame("connack", vmq_parser:gen_connack()),
    compare_frame("connackSessionPresentTrue", vmq_parser:gen_connack(1, 0)),
    compare_frame("connackSessionPresentFalse", vmq_parser:gen_connack(0, 0)),
    compare_frame("publish1", vmq_parser:gen_publish("test-topic", 0, <<"test-payload">>, [{dup, true}, {retain, true}])),
    compare_frame("publish2", vmq_parser:gen_publish("test-topic", 2, crypto:rand_bytes(1000), [{dup, true}, {retain, true}])),
    compare_frame("publish3", vmq_parser:gen_publish("test-topic", 2, crypto:rand_bytes(100000), [{dup, true}, {retain, true}])),
    compare_frame("publish4", vmq_parser:gen_publish("test-topic", 2, crypto:rand_bytes(2097153), [{dup, true}, {retain, true}])),

    compare_frame("puback", vmq_parser:gen_puback(123)),
    compare_frame("pubrec", vmq_parser:gen_pubrec(123)),
    compare_frame("pubrel1", vmq_parser:gen_pubrel(123)),
    compare_frame("pubcomp", vmq_parser:gen_pubcomp(123)),

    compare_frame("subscribe", vmq_parser:gen_subscribe(123, "test/hello/world", 2)),
    compare_frame("subscribe_multi", vmq_parser:gen_subscribe(123, [{"test/1", 1}, {"test/2", 2}])),
    compare_frame("suback", vmq_parser:gen_suback(123, 2)),
    compare_frame("suback_multi", vmq_parser:gen_suback(123, [0,1,2])),
    compare_frame("suback_0x80", vmq_parser:gen_suback(123, not_allowed)),
    compare_frame("suback_multi_0x80", vmq_parser:gen_suback(123, [0,not_allowed,2])),

    compare_frame("unsubscribe", vmq_parser:gen_unsubscribe(123, "test/hello/world")),
    compare_frame("unsuback", vmq_parser:gen_unsuback(123)),

    compare_frame("pingreq", vmq_parser:gen_pingreq()),
    compare_frame("pingresp", vmq_parser:gen_pingresp()),
    compare_frame("disconnect", vmq_parser:gen_disconnect()),


    P = vmq_parser:gen_publish("test-topic", 2, crypto:rand_bytes(100), []),
    {error, packet_exceeds_max_size} = vmq_parser:parse(P, byte_size(P) - 3),

    <<Part:128, _/binary>> = P,
    more = vmq_parser:parse(Part).

compare_frame(Test, Frame) ->
    io:format(user, "---- compare test: ~p~n", [Test]),
    {ParsedFrame, <<>>} = vmq_parser:parse(Frame),
    SerializedFrame = iolist_to_binary(vmq_parser:serialise(ParsedFrame)),
    compare_frame(Test, Frame, SerializedFrame).
compare_frame(_, F, F) -> true.
    
