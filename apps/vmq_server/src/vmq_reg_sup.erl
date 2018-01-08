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

-module(vmq_reg_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_reg_view/1,
         stop_reg_view/1,
         reconfigure_registry/1]).

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
start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    DefaultRegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
    RegViews = lists:usort([DefaultRegView|vmq_config:get_env(reg_views, [])]),
    _ = [{ok, _} = start_reg_view(RV) || RV <- RegViews],
    {ok, Pid}.

reconfigure_registry(Config) ->
    case lists:keyfind(reg_views, 1, Config) of
        {_, RegViews} ->
            DefaultRegView = vmq_config:get_env(default_reg_view, vmq_reg_trie),
            RequiredRegViews = lists:usort([DefaultRegView|RegViews]),
            InstalledRegViews = [Id || {{reg_view, Id}, _, _, _}
                                       <- supervisor:which_children(?MODULE)],
            ToBeInstalled = RequiredRegViews -- InstalledRegViews,
            ToBeUnInstalled = InstalledRegViews -- RequiredRegViews,
            install_reg_views(ToBeInstalled),
            uninstall_reg_views(ToBeUnInstalled);
        false ->
            ok
    end.

install_reg_views([RV|RegViews]) ->
    case start_reg_view(RV) of
        {ok, _} ->
            lager:info("installed reg view ~p", [RV]),
            install_reg_views(RegViews);
        {error, Reason} ->
            lager:error("can't install reg view ~p due to ~p", [RV, Reason]),
            install_reg_views(RegViews)
    end;
install_reg_views([]) -> ok.

uninstall_reg_views([RV|RegViews]) ->
    case stop_reg_view(RV) of
        {error, Reason} ->
            lager:error("can't uninstall reg view ~p due to ~p", [RV, Reason]),
            uninstall_reg_views(RegViews);
        _ ->
            lager:info("uninstalled reg view ~p", [RV]),
            uninstall_reg_views(RegViews)
    end;
uninstall_reg_views([]) -> ok.

start_reg_view(ViewModule) ->
    supervisor:start_child(?MODULE, reg_view_child_spec(ViewModule)).

stop_reg_view(ViewModule) ->
    ChildId = {reg_view, ViewModule},
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        {error, Reason} ->
            {error, Reason}
    end.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, {{one_for_one, 5, 10},[
           ?CHILD(vmq_reg_mgr, vmq_reg_mgr, worker, []),
           ?CHILD(vmq_retain_srv, vmq_retain_srv, worker, []),
           ?CHILD(vmq_reg_sync_action_sup, vmq_reg_sync_action_sup, supervisor, []),
           ?CHILD(vmq_reg_sync, vmq_reg_sync, worker, [])]
         }
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
reg_view_child_spec(ViewModule) ->
    ?CHILD({reg_view, ViewModule}, ViewModule, worker, []).
