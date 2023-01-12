-module(vmq_generic_offline_msg_store_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================
start(_StartType, _StartArgs) ->
    vmq_generic_offline_msg_store_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.
