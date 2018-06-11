-module(vmq_allow_all_v5).

-behaviour(auth_on_register_v1_hook).
-behaviour(auth_on_publish_v1_hook).
-behaviour(auth_on_subscribe_v1_hook).

%% API exports
-export([
         auth_on_register_v1/6,
         auth_on_publish_v1/7,
         auth_on_subscribe_v1/4]).

%%====================================================================
%% API functions
%%====================================================================

auth_on_register_v1(_,_,_,_,_,_) ->
    ok.

auth_on_publish_v1(_,_,_,_,_,_,_) ->
    ok.

auth_on_subscribe_v1(_,_,_,_) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
