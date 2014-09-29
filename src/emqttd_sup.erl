-module(emqttd_sup).

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
    %% we make sure the hooks for emqttd_server are registered first
    emqttd_hook:start(emqttd_server),
    [emqttd_hook:start(A) || {A, _, _}<- application:loaded_applications(),
                             A /= emqttd_server],

    EMQTTDir = "EMQTT."++atom_to_list(node()),
    filelib:ensure_dir(EMQTTDir),
    {ok, { {one_for_one, 5, 10}, [
            ?CHILD(emqttd_endpoint_sup, supervisor, []),
            ?CHILD(emqttd_cluster, worker, []),
            ?CHILD(emqttd_systree, worker, [60000]),
            ?CHILD(emqttd_msg_store, worker, [filename:join(EMQTTDir, "store")])
                                 ]} }.

