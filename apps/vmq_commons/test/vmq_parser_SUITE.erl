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
    [more_data_test].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

more_data_test(_Config) ->
    CompletePacket = packet:gen_publish(<<"a nice topic">>, 1, <<"a complete payload">>, []),
    PartialSize = byte_size(CompletePacket) - 1,
    <<IncompletePacket:PartialSize/binary, _LastByte/binary>> = CompletePacket,
    more = vmq_parser:parse(IncompletePacket).
