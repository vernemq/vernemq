-module(vmq_cth).

%% util functions
-export([ustr/1,
         utopic/1]).

%% Callbacks
-export([init/2]).

-export([pre_init_per_suite/3]).
-export([post_init_per_suite/4]).
-export([pre_end_per_suite/3]).
-export([post_end_per_suite/4]).

-export([pre_init_per_group/3]).
-export([pre_init_per_group/4]).
-export([post_init_per_group/5]).
-export([pre_end_per_group/4]).
-export([post_end_per_group/5]).

-export([pre_init_per_testcase/3]).
-export([pre_init_per_testcase/4]).
-export([post_init_per_testcase/5]).
-export([pre_end_per_testcase/4]).
-export([post_end_per_testcase/5]).

-export([on_tc_fail/4]).
-export([on_tc_skip/4]).

-export([terminate/1]).

-record(state, {}).

%% @doc Always called before any other callback function. Use this to initiate
%% any common state.
init(_Id, _Opts) ->
    {ok, #state{}}.

%% @doc Called before init_per_suite is called.
pre_init_per_suite(_Suite,Config,State) ->
    {Config, State}.

%% @doc Called after init_per_suite.
post_init_per_suite(_Suite,_Config,Return,State) ->
    {Return, State}.

%% @doc Called before end_per_suite.
pre_end_per_suite(_Suite,Config,State) ->
    {Config, State}.

%% @doc Called after end_per_suite.
post_end_per_suite(_Suite,_Config,Return,State) ->
    {Return, State}.

%% @doc Called before each init_per_group. Needed to be compatible
%% with OTP18.
pre_init_per_group(Group,Config,State) ->
    NewConfig = add_metadata(group, Group, Config),
    {NewConfig, State}.

%% @doc Called before each init_per_group.
pre_init_per_group(_Suite,Group,Config,State) ->
    NewConfig = add_metadata(group, Group, Config),
    {NewConfig, State}.

%% @doc Called after each init_per_group.
post_init_per_group(_Suite,_Group,_Config,Return,State) ->
    {Return, State}.

%% @doc Called before each end_per_group.
pre_end_per_group(_Suite,_Group,Config,State) ->
    {Config, State}.

%% @doc Called after each end_per_group.
post_end_per_group(_Suite,_Group,_Config,Return,State) ->
    {Return, State}.

%% @doc Called before each init_per_testcase. Needed to be compatible
%% with OTP18.
pre_init_per_testcase(TC,Config,State) ->
    NewConfig = add_metadata(tc, TC, Config),
    {NewConfig, State}.

%% @doc Called before each init_per_testcase.
pre_init_per_testcase(_Suite,TC,Config,State) ->
    NewConfig = add_metadata(tc, TC, Config),
    {NewConfig, State}.

%% Called after each init_per_testcase (immediately before the test case).
post_init_per_testcase(_Suite,_TC,_Config,Return,State) ->
    {Return, State}.

    %% @doc Called before each end_per_testcase (immediately after the test case).
pre_end_per_testcase(_Suite,_TC,Config,State) ->
    {Config, State}.

%% @doc Called after each end_per_testcase.
post_end_per_testcase(_Suite,_TC,_Config,Return,State) ->
    {Return, State}.

%% @doc Called after post_init_per_suite, post_end_per_suite, post_init_per_group,
%% post_end_per_group and post_end_per_testcase if the suite, group or test case failed.
on_tc_fail(_Suite, _TC, _Reason, State) ->
    State.

%% @doc Called when a test case is skipped by either user action
%% or due to an init function failing.
on_tc_skip(_Suite, _TC, _Reason, State) ->
    State.

%% @doc Called when the scope of the CTH is done
terminate(_State) ->
    ok.

add_metadata(Key, Val, Config) ->
    Md = proplists:get_value(vmq_md, Config, #{}),
    NewConfig = proplists:delete(vmq_md, Config),
    [{vmq_md, Md#{ Key  => Val }} | NewConfig].

ustr(Config) ->
    ustr(":", Config).

utopic(Config) ->
    ustr("/", Config).

ustr(JoinElem, Config) ->
    ustr_(JoinElem, proplists:get_value(vmq_md, Config)).

ustr_(JoinElem, #{group := G, tc := TC}) ->
    lists:flatten([atom_to_list(G), JoinElem, atom_to_list(TC)]).
