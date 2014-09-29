-module(vmq_sup).

-behaviour(supervisor).
%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init([]) -> {'ok',{{'one_for_one',5,10},[{atom(),{atom(),atom(),list()},permanent,pos_integer(),worker,[atom()]}]}}.
init([]) ->
    %% we make sure the hooks for vmq_server are registered first
    vmq_hook:start(vmq_server),
    [vmq_hook:start(A) || {A, _, _}<- application:loaded_applications(),
                             A /= vmq_server],

    EMQTTDir = "EMQTT."++atom_to_list(node()),
    filelib:ensure_dir(EMQTTDir),
    {ok, { {one_for_one, 5, 10}, [
            ?CHILD(vmq_endpoint_sup, supervisor, []),
            ?CHILD(vmq_cluster, worker, []),
            ?CHILD(vmq_systree, worker, [60000]),
            ?CHILD(vmq_msg_store, worker, [filename:join(EMQTTDir, "store")])
                                 ]} }.

