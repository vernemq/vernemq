-module(vmq_ql_SUITE).

%% API
-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).

%% test cases
-export([
         query_prepare_test/1,
         query_select_all_test/1,
         query_select_by_id_test/1,
         query_select_match_test/1,
         query_select_by_pid_test/1
        ]).

%% vmq_ql exports
-export([fields_config/0, fold_init_rows/4]).

-include("vmq_ql.hrl").
-include_lib("common_test/include/ct.hrl").

-define(NR_SAMPLES, 500).

all() ->
    [
     query_prepare_test,
     query_select_all_test,
     query_select_by_id_test,
     query_select_match_test,
     query_select_by_pid_test
    ].


%%%===================================================================
%%% Overall setup/teardown
%%%===================================================================
init_per_suite(Config) ->
    application:set_env(vmq_ql, table_map, [{foobar, ?MODULE},
                                            {modules, ?MODULE},
                                            {proc, vmq_ql_sys_info}]),
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

query_select_match_test(_) ->
    Query = "SELECT * FROM modules WHERE path MATCH \".+" ++ atom_to_list(?MODULE) ++ ".beam\"",
    Pid =  query(Query),
    [{1, #{module := ?MODULE}}] = fetch(Pid, 10).

query_select_by_pid_test(_) ->
    [QPid|_] = erlang:processes(),
    Query = "SELECT * FROM proc WHERE pid = \"" ++ pid_to_list(QPid) ++ "\"",
    Pid = query(Query),
    [{1, #{pid := QPid}}] = fetch(Pid, 10).

query(String) ->
    {ok, Pid} = vmq_ql_query:start_link(self(), String),
    receive
        {results_ready, _, Pid, _} ->
            Pid;
        {query_error, _, Pid, Reason} ->
            {error, Reason}
    end.

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
    Mods = #vmq_ql_table{name=modules,
              provides = [module, path],
              init_fun = fun init_mod_row/1,
              include_if_all = true     %% is included in SELECT * modules
             },
    [Foo, Bar, Mods].

fold_init_rows(foobar, Fun, Acc,_) ->
    lists:foldl(fun(I, AccAcc) ->
                        InitRow = #{id => I},
                        Fun(InitRow, AccAcc)
                end, Acc, lists:seq(1, ?NR_SAMPLES));
fold_init_rows(modules, Fun, Acc,_) ->
    lists:foldl(fun({Mod, Path}, AccAcc) ->
                        InitRow = #{module => Mod, path => Path},
                        Fun(InitRow, AccAcc)
                end, Acc, code:all_loaded()).

init_foo_row(Row) ->
    FooRow = #{a => rand_bool(), b => rand_int(), c => rand_float()},
    [maps:merge(Row, FooRow)].

init_bar_row(Row) ->
    {RandAtom, RandString} = rand_mod(),
    BarRow = #{x => RandAtom, y => rand_pid(), z => RandString},
    [maps:merge(Row, BarRow)].

init_mod_row(Row) -> [Row].


rand_bool() ->
    rand:uniform(10) > 5.

rand_int() ->
    (case rand_bool() of true -> 1; false -> -1 end) * rand:uniform(1000).

rand_float() ->
    rand_int() / rand_int().

rand_mod() ->
    Mods = code:all_loaded(),
    lists:nth(rand:uniform(length(Mods)), Mods).

rand_pid() ->
    Pids = erlang:processes(),
    lists:nth(rand:uniform(length(Pids)), Pids).








