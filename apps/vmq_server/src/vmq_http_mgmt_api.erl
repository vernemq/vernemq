%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_http_mgmt_api).
-behaviour(vmq_http_config).
-include_lib("kernel/include/logger.hrl").

%% cowboy rest handler callbacks
-export([
    init/2,
    allowed_methods/2,
    content_types_provided/2,
    options/2,
    is_authorized/2,
    malformed_request/2,
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

options(Req0, State) ->
    %% Set CORS Headers
    CorsHeaders = #{
        <<"access-control-max-age">> => <<"1728000">>,
        <<"access-control-allow-methods">> => <<"HEAD, GET">>,
        <<"access-control-allow-headers">> => <<"content-type, authorization">>,
        <<"access-control-allow-origin">> => <<$*>>
    },
    {ok, cowboy_req:set_resp_headers(CorsHeaders, Req0), State}.

is_authorized(Req, State) ->
    AuthMode = vmq_http_config:auth_mode(Req, vmq_http_mgmt_api),
    case AuthMode of
        "apikey" -> vmq_auth_apikey:is_authorized(Req, State, "mgmt");
        "noauth" -> {true, Req, State};
        _ -> {error, invalid_authentication_scheme}
    end.

malformed_request(Req, State) ->
    PathInfo = cowboy_req:path_info(Req),
    QsVals = cowboy_req:parse_qs(Req),
    try validate_command(PathInfo, QsVals) of
        {error, V} ->
            ?LOG_ERROR("malformed request ~p", [V]),
            {true, Req, State};
        M3 ->
            {false, Req, M3}
    catch
        _:_ ->
            {true, Req, State}
    end.

to_json(Req, State) ->
    CmdOut = run_command(State),
    case clique_writer:write(CmdOut, "json") of
        {StdOut, []} ->
            {iolist_to_binary(StdOut), Req, undefined};
        {[], StdErr} ->
            Reply = cowboy_req:reply(400, #{}, <<"invalid_request_error">>, Req),
            {iolist_to_binary(StdErr), Reply, State}
    end.

validate_command(Command, QsVals) ->
    ParamsAndFlags = parse_qs(QsVals),
    M0 = clique_command:match(
        parse_command(Command) ++
            ParamsAndFlags ++
            ["--format=json"]
    ),
    M1 = clique_parser:parse(M0),
    M2 = clique_parser:extract_global_flags(M1),
    clique_parser:validate(M2).

parse_qs(QsVals) ->
    parse_qs(QsVals, []).

parse_qs([{<<"--", Flag/binary>>, true} | Rest], Acc) ->
    parse_qs(Rest, ["--" ++ binary_to_list(Flag) | Acc]);
parse_qs([{<<"--", Opt/binary>>, Val} | Rest], Acc) ->
    parse_qs(Rest, ["--" ++ binary_to_list(Opt) ++ "=" ++ binary_to_list(Val) | Acc]);
parse_qs([{Param, Val} | Rest], Acc) ->
    parse_qs(Rest, [binary_to_list(Param) ++ "=" ++ binary_to_list(Val) | Acc]);
parse_qs([], Acc) ->
    lists:reverse(Acc).

run_command(M3) ->
    {Res, _, _} = clique_command:run(M3),
    Res.

%% Command Aliases
parse_command([<<"cluster">>]) -> parse_command([<<"cluster">>, <<"status">>]);
parse_command([<<"sessions">>]) -> parse_command([<<"session">>, <<"show">>]);
parse_command([<<"sessions">> | Rest]) -> parse_command([<<"session">> | Rest]);
parse_command([<<"listeners">>]) -> parse_command([<<"listener">>, <<"show">>]);
parse_command([<<"listeners">> | Rest]) -> parse_command([<<"listener">> | Rest]);
parse_command([<<"plugins">>]) -> parse_command([<<"plugin">>, <<"show">>]);
parse_command([<<"plugins">> | Rest]) -> parse_command([<<"plugin">> | Rest]);
parse_command(Command) -> ["vmq-admin" | [binary_to_list(C) || C <- Command]].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
routes() ->
    [{"/api/v1/[...]", ?MODULE, []}].
