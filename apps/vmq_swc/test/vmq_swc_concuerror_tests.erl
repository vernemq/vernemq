-module(vmq_swc_concuerror_tests).

-include("vmq_swc.hrl").
% Test a written value can be read back
-export([write_and_read_test/0]).

write_and_read_test() ->
    Key = <<"1">>,
    Value = <<"value">>,
    Config = #swc_config{peer = node(),
                        group = vmq_swc_meta2},
    vmq_swc_store:write(Config, Key, Value),
    <<"value">> = vmq_swc_store:read(Config, Key).