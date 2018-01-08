%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_diversity_script_sup_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1,
         reload_script/1,
         stats/1,
         start_state/3,
         get_state_mgr/1]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Script) ->
    {ok, ScriptSup} = Ret = supervisor:start_link(?MODULE, []),
    LuaStatePids = setup_lua_states(ScriptSup, Script),
    {ok, StateMgrPid} = start_state_mgr(ScriptSup, Script, LuaStatePids),
    maybe_register_hooks(StateMgrPid, LuaStatePids),
    Ret.

reload_script(SupPid) ->
    lists:foreach(fun
                    ({{vmq_diversity_script_state, _}, Child, _, _}) when is_pid(Child) ->
                          vmq_diversity_script_state:reload(Child);
                    (_) ->
                          ignore
                  end, supervisor:which_children(SupPid)).

stats(SupPid) ->
    lists:foldl(fun
                    ({{vmq_diversity_script, Script}, Child, _, _}, Acc) when is_pid(Child) ->
                        [{Script, vmq_diversity_script:stats(Child)}|Acc];
                    (_, Acc) ->
                        Acc
                end, [], supervisor:which_children(SupPid)).

start_state_mgr(SupPid, Script, StatePids) ->
    supervisor:start_child(SupPid, ?CHILD({vmq_diversity_script, Script},
                                          vmq_diversity_script, worker, [StatePids])).

start_state(SupPid, Id, Script) ->
    supervisor:start_child(SupPid, ?CHILD({vmq_diversity_script_state, Id},
                                          vmq_diversity_script_state, worker,
                                          [Id, Script])).

get_state_mgr(SupPid) ->
    lists:foldl(fun
                    ({{vmq_diversity_script, _}, Child, _, _}, undefined)
                      when is_pid(Child) ->
                        Child;
                    (_, Acc) ->
                        Acc
                end, undefined, supervisor:which_children(SupPid)).



%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    {ok, {{one_for_all, 5, 10}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
setup_lua_states(ScriptSup, Script) ->
    {ok, FirstLuaStatePid} = start_state(ScriptSup, 1, Script),
    case vmq_diversity_script_state:get_num_states(FirstLuaStatePid) of
        1 ->
            [FirstLuaStatePid];
        NumLuaStates when NumLuaStates > 1 ->
            LuaStatePids =
            lists:foldl(
              fun(Id, Acc) ->
                      {ok, LuaStatePid} = start_state(ScriptSup, Id, Script),
                      [LuaStatePid|Acc]
              end, [FirstLuaStatePid], lists:seq(2, NumLuaStates)),
            lists:reverse(LuaStatePids)
    end.

maybe_register_hooks(StateMgrPid, [FirstLuaStatePid|_]) ->
    case vmq_diversity_script_state:get_hooks(FirstLuaStatePid) of
        [] ->
            ok;
        Hooks when is_list(Hooks) ->
            lists:foreach(
              fun(Hook) ->
                      vmq_diversity_plugin:register_hook(StateMgrPid, Hook)
              end, Hooks)
    end.

