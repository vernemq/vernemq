%% Copyright 2023- Octavo Labs AG, Switzerland
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

-module(vmq_http_v2_api).
-behaviour(vmq_http_config).
-include("vmq_server.hrl").

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
    OnlineStatus = get_online_status(SubscriberId),
    Reply0 =
        case TopicFlag of
            true ->
                Topics = get_topics(SubscriberId),
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

get_online_status(SubscriberId) ->
    QueuePid = vmq_queue_sup_sup:get_queue_pid(SubscriberId),
    case QueuePid of
        not_found ->
            <<"unknown">>;
        _ ->
            #{is_online := IsOnline} = vmq_queue:info(QueuePid),
            case IsOnline of
                true -> <<"online">>;
                _ -> <<"offline">>
            end
    end.

get_topics(SubscriberId) ->
    Res = vmq_subscriber_db:read(SubscriberId),
    case Res of
        undefined ->
            [];
        [{_, _, []}] ->
            [];
        [{_, _, Topics}] ->
            [{iolist_to_binary(vmq_topic:unword(Topic)), QoS} || {Topic, QoS} <- Topics]
    end.

force_disconnect(SubscriberId, DoCleanup) ->
    QueuePid = vmq_queue_sup_sup:get_queue_pid(SubscriberId),
    case QueuePid of
        not_found -> not_found;
        _ -> vmq_queue:force_disconnect(QueuePid, ?ADMINISTRATIVE_ACTION, DoCleanup)
    end.
