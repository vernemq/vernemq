%% Copyright 2024 Octavo Labs/VerneMQ (https://vernemq.com/)
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

-module(vmq_balance_http).

-behaviour(vmq_http_config).

-export([routes/0, is_authorized/2]).
-export([init/2]).

routes() ->
    [{"/api/balance-health", ?MODULE, []}].

init(Req, Opts) ->
    {IsAccepting, LocalConnections, ClusterAvg, _IsEnabled} =
        vmq_balance_srv:balance_stats(),
    {Code, Status} =
        case IsAccepting of
            1 -> {200, <<"accepting">>};
            0 -> {503, <<"rejecting">>}
        end,
    Payload = [
        {<<"status">>, Status},
        {<<"connections">>, LocalConnections},
        {<<"cluster_avg">>, ClusterAvg}
    ],
    Headers = #{<<"content-type">> => <<"application/json">>},
    cowboy_req:reply(Code, Headers, vmq_json:encode(Payload), Req),
    {ok, Req, Opts}.

is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, vmq_balance_http),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "balance");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.
