-module(vmq_diversity_script_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0,
         start_script/1,
         reload_script/1,
         stop_script/1,
         stats/0]).

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
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_script(Script) ->
    supervisor:start_child(?MODULE, ?CHILD({vmq_diversity_script, Script},
                                           vmq_diversity_script, worker, [Script])).

reload_script(Script) ->
    case lists:keyfind({vmq_diversity_script, Script}, 1,
                       supervisor:which_children(?MODULE)) of
        {_, Pid, worker, _} when is_pid(Pid) ->
            vmq_diversity_script:reload(Pid);
        _ ->
            {error, script_not_found}
    end.

stop_script(Script) ->
    case supervisor:terminate_child(?MODULE, {vmq_diversity_script, Script}) of
        ok ->
            supervisor:delete_child(?MODULE, {vmq_diversity_script, Script});
        E ->
            E
    end.

stats() ->
    lists:foldl(fun
                    ({{vmq_diversity_script, Script}, Child, _, _}, Acc) when is_pid(Child) ->
                        [{Script, vmq_diversity_script:stats(Child)}|Acc];
                    (_, Acc) ->
                        Acc
                end, [], supervisor:which_children(?MODULE)).


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
    {ok, {{one_for_one, 5, 10}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
