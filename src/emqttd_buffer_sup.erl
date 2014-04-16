-module(emqttd_buffer_sup).
-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Name, Mod, Type, Args), {Name, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link(BufferDir, NrOfBuffers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [BufferDir, NrOfBuffers]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


init([BufferDir, NrOfBuffers]) ->
    Specs =
    [begin
         Name = list_to_atom("buffer-"++integer_to_list(I)),
         ?CHILD(Name, emqttd_buffer, worker, [BufferDir, Name])
     end || I <- lists:seq(1, NrOfBuffers)],
    {ok, { {one_for_one, 5, 10}, Specs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

