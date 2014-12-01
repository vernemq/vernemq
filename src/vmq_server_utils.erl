-module(vmq_server_utils).
-export([total_clients/0,
         in_flight/0,
         retained/0,
         stored/0,
         active_clients/0]).

total_clients() ->
    vmq_reg:total_clients().

in_flight() ->
    vmq_msg_store:in_flight().

retained() ->
    vmq_reg:retained().

stored() ->
    vmq_msg_store:stored().

active_clients() ->
    vmq_session_sup:active_clients().

