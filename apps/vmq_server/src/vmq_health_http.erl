%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_health_http).
-include_lib("kernel/include/logger.hrl").

-behaviour(vmq_http_config).

-export([routes/0, is_authorized/2]).
-export([init/2]).

routes() ->
    [
        {"/health", ?MODULE, []},
        {"/health/ping", ?MODULE, []},
        {"/health/listeners", ?MODULE, []},
        {"/health/listeners_full_cluster", ?MODULE, []}
    ].

init(Req, Opts) ->
    Path = cowboy_req:path(Req),
    {Code, Payload} =
        case Path of
            <<"/health/ping">> ->
                {200, [{<<"status">>, <<"OK">>}]};
            <<"/health">> ->
                case check_health_concerns() of
                    [] ->
                        {200, [{<<"status">>, <<"OK">>}]};
                    Concerns ->
                        {503, [
                            {<<"status">>, <<"DOWN">>},
                            {<<"reasons">>, Concerns}
                        ]}
                end;
            <<"/health/listeners">> ->
                case listeners_status() of
                    ok ->
                        {200, [{<<"status">>, <<"OK">>}]};
                    {error, Reason} ->
                        {503, [
                            {<<"status">>, <<"DOWN">>},
                            {<<"reason">>, Reason}
                        ]}
                end;
            <<"/health/listeners_full_cluster">> ->
                case check_full_health_concerns() of
                    [] ->
                        {200, [{<<"status">>, <<"OK">>}]};
                    Concerns ->
                        {503, [
                            {<<"status">>, <<"DOWN">>},
                            {<<"reasons">>, Concerns}
                        ]}
                end
        end,
    Headers = #{<<"content-type">> => <<"application/json">>},
    cowboy_req:reply(Code, Headers, vmq_json:encode(Payload), Req),
    {ok, Req, Opts}.

is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, vmq_health_http),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "health");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.

-spec check_health_concerns() -> [] | [Concern :: string()].
check_health_concerns() ->
    lists:filtermap(
        fun(Status) ->
            case Status of
                ok -> false;
                {error, Reason} -> {true, list_to_binary(Reason)}
            end
        end,
        [cluster_status(), listeners_status()]
    ).

-spec cluster_status() -> ok | {error, Reason :: string()}.
cluster_status() ->
    ThisNode = node(),
    try
        case vmq_cluster:status() of
            [] ->
                {error, "Unknown cluster status"};
            Status ->
                case lists:keyfind(ThisNode, 1, Status) of
                    {ThisNode, true} -> ok;
                    false -> {error, "Node has not joined cluster"}
                end
        end
    catch
        Exception:Reason ->
            ?LOG_DEBUG("Cluster status check failed ~p:~p", [Exception, Reason]),
            {error, "Unknown cluster status"}
    end.

-spec listeners_status() -> ok | {error, Reason :: string()}.
listeners_status() ->
    NotRunningListeners = lists:filtermap(
        fun({Type, _, _, Status, _, _, _, _}) ->
            case Status of
                running ->
                    false;
                _ ->
                    {true, Type}
            end
        end,
        vmq_ranch_config:listeners()
    ),
    case NotRunningListeners of
        [] ->
            ok;
        Listeners ->
            {error, io_lib:format("Listeners are not ready: ~p", [Listeners])}
    end.

cluster_full_status() ->
    try
        case vmq_cluster:status() of
            [] ->
                {error, "Unknown cluster status"};
            Status ->
                case lists:member(false, [Online || {_Node, Online} <- Status]) of
                    false -> ok;
                    true -> {error, "At least one node is offline"}
                end
        end
    catch
        Exception:Reason ->
            ?LOG_DEBUG("Cluster status check failed ~p:~p", [Exception, Reason]),
            {error, "Unknown cluster status"}
    end.
-spec check_full_health_concerns() -> [] | [Concern :: string()].
check_full_health_concerns() ->
    lists:filtermap(
        fun(Status) ->
            case Status of
                ok -> false;
                {error, Reason} -> {true, list_to_binary(Reason)}
            end
        end,
        [cluster_full_status(), listeners_status()]
    ).
