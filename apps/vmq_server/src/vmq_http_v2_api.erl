%% Copyright 2023-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
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

-module(vmq_http_v2_api).
-behaviour(vmq_http_config).
-include("vmq_server.hrl").
-include_lib("kernel/include/logger.hrl").

%% cowboy rest handler callbacks
-export([
    init/2,
    allowed_methods/2,
    content_types_provided/2,
    options/2,
    is_authorized/2,
    to_json/2
]).

-export([
    routes/0
]).

-define(RPC_TIMEOUT, 60000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy REST Handler
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"OPTIONS">>, <<"HEAD">>], Req, State}.

content_types_provided(Req, State) ->
    {[{{<<"application">>, <<"json">>, '*'}, to_json}], Req, State}.

options(Req, State) ->
    %% Set CORS Headers
    CorsHeaders = #{
        <<"access-control-max-age">> => <<"1728000">>,
        <<"access-control-allow-methods">> => <<"HEAD, GET">>,
        <<"access-control-allow-headers">> => <<"content-type, authorization">>,
        <<"access-control-allow-origin">> => <<$*>>
    },
    {ok, cowboy_req:set_resp_headers(CorsHeaders, Req), State}.

is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, ?MODULE),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "api2");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.

to_json(Req, State) ->
    Command = cowboy_req:binding(command, Req),
    handle_command(Command, Req, State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
routes() ->
    [{"/api/v2/:command", ?MODULE, []}].

%% internal calls
handle_command(<<"client">>, Req, State) ->
    Qs = cowboy_req:parse_qs(Req),
    SubscriberId = subscriber_from_qs(Qs),
    TopicFlag = proplists:get_value(<<"topics">>, Qs, false),
    {OnlineStatus, Topics} = get_status_and_topics(SubscriberId),
    Reply0 =
        case TopicFlag of
            true ->
                [{<<"status">>, OnlineStatus}, {<<"topics">>, Topics}];
            _ ->
                [{<<"status">>, OnlineStatus}]
        end,
    ReplyReq = cowboy_req:reply(200, #{}, vmq_json:encode(Reply0), Req),
    {stop, ReplyReq, State};
handle_command(<<"disconnect">>, Req, State) ->
    Qs = cowboy_req:parse_qs(Req),
    SubscriberId = subscriber_from_qs(Qs),
    DoCleanup = proplists:get_value(<<"cleanup">>, Qs, false),
    Reply0 = force_disconnect(SubscriberId, DoCleanup),
    ReplyReq = cowboy_req:reply(200, #{}, vmq_json:encode(Reply0), Req),
    {stop, ReplyReq, State};
handle_command(_Command, Req, State) ->
    Reply = cowboy_req:reply(400, #{}, <<"invalid_command">>, Req),
    {stop, Reply, State}.

subscriber_from_qs(Qs) ->
    Mountpoint = proplists:get_value(<<"mountpoint">>, Qs, []),
    ClientId = proplists:get_value(<<"client_id">>, Qs),
    {Mountpoint, ClientId}.

force_disconnect(SubscriberId, DoCleanup) ->
    Res = vmq_subscriber_db:read(SubscriberId),
    case Res of
        undefined ->
            <<"unknown">>;
        [{Node, _, _}] ->
            try erpc:call(Node, vmq_queue_sup_sup, get_queue_pid, [SubscriberId], ?RPC_TIMEOUT) of
                not_found ->
                    not_found;
                QPid when is_pid(QPid) ->
                    vmq_queue:force_disconnect(QPid, ?ADMINISTRATIVE_ACTION, DoCleanup)
            catch
                E:R ->
                    ?LOG_DEBUG("API v2 disconnect RPC failed with ~p:~p", [E, R]),
                    {error, rpc_fail}
            end
    end.
% {Status, Topics}
get_status_and_topics(SubscriberId) ->
    Res = vmq_subscriber_db:read(SubscriberId),
    case Res of
        undefined ->
            {<<"unknown">>, []};
        [{Node, _, Topics}] ->
            try erpc:call(Node, vmq_queue_sup_sup, get_queue_pid, [SubscriberId], ?RPC_TIMEOUT) of
                % we should not end up here, vmq_subscriber_db gave wrong Node
                not_found ->
                    {<<"no_queue">>, []};
                QPid when is_pid(QPid) ->
                    #{is_online := IsOnline} = vmq_queue:info(QPid),
                    FlatTopics =
                        [
                            {iolist_to_binary(vmq_topic:unword(Topic)), QoS}
                         || {Topic, QoS} <- Topics
                        ],
                    case IsOnline of
                        true -> {<<"online">>, FlatTopics};
                        _ -> {<<"offline">>, FlatTopics}
                    end
            catch
                E:R ->
                    ?LOG_DEBUG("API v2 client status RPC failed with ~p:~p", [E, R]),
                    {error, rpc_fail}
            end
    end.
