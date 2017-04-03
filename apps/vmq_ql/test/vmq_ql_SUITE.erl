-module(vmq_ql_SUITE).

%% API
-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).

%% test cases
-export([
         query_prepare_test/1
        ]).

%% vmq_ql exports
-export([fields_config/0, fold_init_rows/2]).

-include("vmq_ql.hrl").
-include_lib("common_test/include/ct.hrl").


all() ->
    [
     query_prepare_test
    ].


%%%===================================================================
%%% Overall setup/teardown
%%%===================================================================
init_per_suite(Config) ->
    application:set_env(vmq_ql, table_map, [{test, ?MODULE}]),
    Config.

end_per_suite(_Config) ->
    ok.


%%%===================================================================
%%% Individual Test Cases (from groups() definition)
%%%===================================================================
%%%
query_prepare_test(_) ->
    Parsed = vmq_ql_parser:parse("SELECT x, y FROM test WHERE (1=c) AND a=1 AND b=2"),

    io:format(user, "~p~n", [Parsed]),
    _Fields = proplists:get_value(fields, Parsed),
    Where = proplists:get_value(where, Parsed),

    [a,b,c] = vmq_ql_query:required_fields(Where).



%%%%-----VMQ_QL TABLE MODULE ------
fields_config() ->
    #vmq_ql_table{name=foo,
                  depends_on=[#vmq_ql_table{
                                 name=bar,
                                 provides = [x,y,z],
                                 init_fun = fun init_bar/0
                                }],
                  provides = [a,b,c],
                  init_fun = fun init_foo/0
                  }.

fold_init_rows(_Fun, _Acc) ->
    ok.

init_foo() ->
    ok.
init_bar() ->
    ok.

