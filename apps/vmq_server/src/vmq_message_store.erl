%% Copyright 2019 Octavo Labs AG Zurich Switzerland (http://octavolabs.com)
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

-module(vmq_message_store).
-include("vmq_server.hrl").
-export([start/0,
         stop/0,
         write/2,
         read/2,
         delete/1,
         delete/2,
         find/1]).

-export([write_with_retry/3,
         read_with_retry/3,
         delete_all_with_retry/2,
         delete_with_retry/3,
         find_with_retry/2]).

-define(RETRY_INTERVAL, application:get_env(vmq_server, message_store_retry_interval, 2000)).
-define(NR_OF_RETRIES, application:get_env(vmq_server, message_store_nr_of_retries, 2)).

start() ->
    Impl = application:get_env(vmq_server, message_store_impl, vmq_generic_offline_msg_store),
    Ret = vmq_plugin_mgr:enable_system_plugin(Impl, [internal]),
    lager:info("Try to start ~p: ~p", [Impl, Ret]),
    Ret.

stop() ->
    % vmq_message_store:stop is typically called when stopping the vmq_server
    % OTP application. As vmq_plugin_mgr:disable_plugin is likely stopping
    % another OTP application too we might block the OTP application
    % controller. Wrapping the disable_plugin in its own process would
    % enable to stop the involved applications. Moreover, because an
    % application:stop is actually a gen_server:call to the application
    % controller the order of application termination is still provided.
    % Nevertheless, this is of course only a workaround and the problem
    % needs to be addressed when reworking the plugin system.
    Impl = application:get_env(vmq_server, message_store_impl, vmq_generic_offline_msg_store),
    _ = spawn(fun() ->
                      Ret = vmq_plugin_mgr:disable_plugin(Impl),
                      lager:info("Try to stop ~p: ~p", [Impl, Ret])
              end),
    ok.

write(SubscriberId, Msg) ->
    vmq_util:timed_measurement({?MODULE, write}, ?MODULE, write_with_retry, [SubscriberId, Msg, ?NR_OF_RETRIES]).
write_with_retry(SubscriberId, Msg, N) when N >= 0 ->
    case vmq_plugin:only(msg_store_write, [SubscriberId, Msg]) of
        {ok, _Count} -> ok;
        {error, no_matching_hook_found} = ErrRes -> ErrRes;
        {error, Err} ->
            lager:error("Error: ~p", [Err]),
            vmq_metrics:incr_msg_store_ops_error(write),
            timer:sleep(?RETRY_INTERVAL),
            write_with_retry(SubscriberId, Msg, N-1)
    end;
write_with_retry(_SubscriberId, _Msg, _N) ->
    vmq_metrics:incr_msg_store_retry_exhausted(write),
    ok.

read(SubscriberId, MsgRef) ->
    vmq_util:timed_measurement({?MODULE, read}, ?MODULE, read_with_retry, [SubscriberId, MsgRef, ?NR_OF_RETRIES]).
read_with_retry(SubscriberId, MsgRef, N) when N >= 0 ->
    case vmq_plugin:only(msg_store_read, [SubscriberId, MsgRef]) of
        {ok, _} = OkRes -> OkRes;
        {error, no_matching_hook_found} = ErrRes -> ErrRes;
        {error, not_supported} = ErrRes -> ErrRes;
        {error, Err} ->
            lager:error("Error: ~p", [Err]),
            vmq_metrics:incr_msg_store_ops_error(read),
            timer:sleep(?RETRY_INTERVAL),
            read_with_retry(SubscriberId, MsgRef, N-1)
    end;
read_with_retry(_SubscriberId, _MsgRef, _N) ->
    vmq_metrics:incr_msg_store_retry_exhausted(read),
    {error, retry_exhausted}.

delete(SubscriberId) ->
    vmq_util:timed_measurement({?MODULE, delete_all}, ?MODULE, delete_all_with_retry, [SubscriberId, ?NR_OF_RETRIES]).
delete_all_with_retry(SubscriberId, N) when N >= 0 ->
    case vmq_plugin:only(msg_store_delete, [SubscriberId]) of
        {ok, _Count} -> ok;
        {error, no_matching_hook_found} = ErrRes -> ErrRes;
        {error, Err} ->
            lager:error("Error: ~p", [Err]),
            vmq_metrics:incr_msg_store_ops_error(delete_all),
            timer:sleep(?RETRY_INTERVAL),
            delete_all_with_retry(SubscriberId, N-1)
    end;
delete_all_with_retry(_SubscriberId, _N) ->
    vmq_metrics:incr_msg_store_retry_exhausted(delete_all),
    ok.

delete(SubscriberId, MsgRef) ->
    vmq_util:timed_measurement({?MODULE, delete}, ?MODULE, delete_with_retry, [SubscriberId, MsgRef, ?NR_OF_RETRIES]).
delete_with_retry(SubscriberId, MsgRef, N) when N >= 0 ->
    case vmq_plugin:only(msg_store_delete, [SubscriberId, MsgRef]) of
        {ok, _Count} -> ok;
        {error, no_matching_hook_found} = ErrRes -> ErrRes;
        {error, Err} ->
            lager:error("Error: ~p", [Err]),
            vmq_metrics:incr_msg_store_ops_error(delete),
            timer:sleep(?RETRY_INTERVAL),
            delete_with_retry(SubscriberId, MsgRef, N-1)
    end;
delete_with_retry(_SId, _MsgRef, _N) ->
    vmq_metrics:incr_msg_store_retry_exhausted(delete),
    ok.

find(SubscriberId) ->
    vmq_util:timed_measurement({?MODULE, find}, ?MODULE, find_with_retry, [SubscriberId, ?NR_OF_RETRIES]).
find_with_retry(SubscriberId, N) when N >= 0 ->
    case vmq_plugin:only(msg_store_find, [SubscriberId]) of
        {ok, _} = OkRes -> OkRes;
        {error, no_matching_hook_found} = ErrRes -> ErrRes;
        {error, Err} ->
            lager:error("Error: ~p", [Err]),
            vmq_metrics:incr_msg_store_ops_error(find),
            timer:sleep(?RETRY_INTERVAL),
            find_with_retry(SubscriberId, N-1)
    end;
find_with_retry(_SId, _N) ->
    vmq_metrics:incr_msg_store_retry_exhausted(find),
    {error, retry_exhausted}.
