-module(vmq_ql_SUITE).

%% API
-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).

%% test cases
-export([
         query_prepare_test/1,
         query_select_all_test/1,
         query_select_by_id_test/1
        ]).

%% vmq_ql exports
-export([fields_config/0, fold_init_rows/2]).

-include("vmq_ql.hrl").
-include_lib("common_test/include/ct.hrl").

-define(NR_SAMPLES, 1000).

all() ->
    [
     query_prepare_test,
     query_select_all_test
     %query_select_by_id_test
    ].


%%%===================================================================
%%% Overall setup/teardown
%%%===================================================================
init_per_suite(Config) ->
    application:set_env(vmq_ql, table_map, [{foobar, ?MODULE}]),
    Config.

end_per_suite(_Config) ->
    ok.


%%%===================================================================
%%% Individual Test Cases (from groups() definition)
%%%===================================================================
%%%
query_prepare_test(_) ->
    Parsed = vmq_ql_parser:parse("SELECT x, y FROM test WHERE (1=c) AND a=1 AND b=2"),
    Where = proplists:get_value(where, Parsed),
    [a,b,c] = vmq_ql_query:required_fields(Where).

query_select_all_test(_) ->
    Query1 = "SELECT * FROM foobar",
    Pid = query(Query1),
    Limit = 10,
    lists:foreach(fun(_) ->
                          Limit = length(fetch(Pid, Limit))
                  end, lists:seq(1, ?NR_SAMPLES div Limit)).

query_select_by_id_test(_) ->
    BaseQuery = "SELECT id FROM foobar WHERE id = ",
    Limit = 10,
    lists:foreach(fun(Id) ->
                          Pid = query(BaseQuery ++ integer_to_list(Id)),
                          [{1, #{id := Id}}] = fetch(Pid, Limit)
                  end, lists:seq(1, ?NR_SAMPLES)).


query(String) ->
    {ok, Pid} = vmq_ql_query:start_link(self(), String),
    Pid.

fetch(Pid, Limit) ->
    vmq_ql_query:fetch(Pid, desc, Limit).


%%%%-----VMQ_QL TABLE MODULE ------
fields_config() ->
    Foo = #vmq_ql_table{name=foo,
                  provides = [id, a,b,c],
                  init_fun = fun init_foo_row/1,
                  include_if_all = true     %% is included in SELECT * foobar
                  },
    Bar = #vmq_ql_table{
             name=bar,
             depends_on=[Foo],              %% requires FOO to be present
             provides = [x,y,z],
             init_fun = fun init_bar_row/1,
             include_if_all = false         %% is excluded in SELECT * foobar
            },
    [Foo, Bar].

fold_init_rows(Fun, Acc) ->
    lists:foldl(fun(I, AccAcc) ->
                        InitRow = #{id => I},
                        Fun(InitRow, AccAcc)
                end, Acc, lists:seq(1, ?NR_SAMPLES)).

init_foo_row(Row) ->
    FooRow = #{a => rand_bool(), b => rand_int(), c => rand_float()},
    [maps:merge(Row, FooRow)].

init_bar_row(Row) ->
    {RandAtom, RandString} = rand_mod(),
    BarRow = #{x => RandAtom, y => rand_pid(), z => RandString},
    [maps:merge(Row, BarRow)].


rand_bool() ->
    random:uniform(10) > 5.

rand_int() ->
    (case rand_bool() of true -> 1; false -> -1 end) * random:uniform(1000).

rand_float() ->
    rand_int() / rand_int().

rand_mod() ->
    Mods = code:all_loaded(),
    lists:nth(random:uniform(length(Mods)), Mods).

rand_pid() ->
    Pids = erlang:processes(),
    lists:nth(random:uniform(length(Pids)), Pids).








