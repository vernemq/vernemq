-module(vmq_allow_all_v5).

-behaviour(auth_on_register_m5_hook).
-behaviour(auth_on_publish_m5_hook).
-behaviour(auth_on_subscribe_m5_hook).

%% API exports
-export([
         auth_on_register_m5/6,
         auth_on_publish_m5/7,
         auth_on_subscribe_m5/4]).

%%====================================================================
%% API functions
%%====================================================================

auth_on_register_m5(_,_,_,_,_,_) ->
    ok.

auth_on_publish_m5(_,_,_,_,_,_,_) ->
    ok.

auth_on_subscribe_m5(_,_,_,_) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
