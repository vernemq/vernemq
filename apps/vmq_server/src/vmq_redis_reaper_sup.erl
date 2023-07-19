-module(vmq_redis_reaper_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    ensure_reaper/1,
    del_reaper/1,
    get_reaper/1
]).

%% Supervisor callbacks
-export([init/1]).

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

ensure_reaper(Node) ->
    case get_reaper(Node) of
        {error, not_found} ->
            {ok, _} = supervisor:start_child(?MODULE, child_spec(Node)),
            ok;
        {ok, _} ->
            ok
    end.

del_reaper(Node) ->
    ChildId = {vmq_redis_reaper, Node},
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        {error, not_found} ->
            {error, not_found}
    end.

get_reaper(Node) ->
    ChildId = {vmq_redis_reaper, Node},
    case lists:keyfind(ChildId, 1, supervisor:which_children(?MODULE)) of
        false ->
            {error, not_found};
        {_, undefined, _, _} ->
            %% child was stopped
            {error, not_found};
        {_, restarting, _, _} ->
            %% child is restarting
            timer:sleep(100),
            get_reaper(Node);
        {_, Pid, _, _} when is_pid(Pid) ->
            {ok, Pid}
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
    {ok, {{one_for_one, 5, 10}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
child_spec(Node) ->
    {{vmq_redis_reaper, Node}, {vmq_redis_reaper, start_link, [Node]}, transient, 5000, worker, [
        vmq_redis_reaper
    ]}.
