-module(vmq_parser_mqtt5_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

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
    [parse_unparse_tests].


parse_unparse_tests(_Config) ->
    parse_unparse("connect", vmq_parser_mqtt5:gen_connect("test-client", [])).

parse_unparse(Test, Frame) ->
    io:format(user, "parse/unparse: ~p~n", [Test]),
    {ParsedFrame, <<>>} = vmq_parser_mqtt5:parse(Frame),
    SerializedFrame = iolist_to_binary(vmq_parser_mqtt5:serialise(ParsedFrame)),
    compare_frame(Test, Frame, SerializedFrame).

compare_frame(_, F, F) -> true.

