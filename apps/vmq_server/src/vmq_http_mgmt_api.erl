%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_http_mgmt_api).
-behaviour(vmq_http_config).

%% cowboy rest handler callbacks
-export([init/3,
         rest_init/2,
         allowed_methods/2,
         content_types_provided/2,
         options/2,
         is_authorized/2,
         malformed_request/2,
         to_json/2]).

-export([routes/0,
         create_api_key/0,
         add_api_key/1,
         delete_api_key/1,
         list_api_keys/0]).

-define(ENV_API_KEYS, http_mgmt_api_keys).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy REST Handler
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(_Transport, _Req, _Opts) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Opts) ->
    {ok, Req, undefined}.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"OPTIONS">>, <<"HEAD">>], Req, State}.

content_types_provided(Req, State) ->
    {[{<<"application/json">>, to_json}], Req, State}.

options(Req0, State) ->
    %% CORS Headers
    Req1 = cowboy_req:set_resp_header(<<"access-control-max-age">>, <<"1728000">>, Req0),
    Req2 = cowboy_req:set_resp_header(<<"access-control-allow-methods">>, <<"HEAD, GET">>, Req1),
    Req3 = cowboy_req:set_resp_header(<<"access-control-allow-headers">>, <<"content-type, authorization">>, Req2),
    Req4 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>, <<$*>>, Req3),

    {ok, Req4, State}.

is_authorized(Req, State) ->
    {ok, Auth, Req1} = cowboy_req:parse_header(<<"authorization">>, Req),
    case Auth of
        {<<"basic">>, {ApiKey, _}} ->
            case lists:member(ApiKey, list_api_keys()) of
                true ->
                    {true, Req1, State};
                false ->
                    {{false, <<"Basic realm=\"VerneMQ\"">>}, Req1, State}
            end;
        _ ->
            {{false, <<"Basic realm=\"VerneMQ\"">>}, Req1, State}
    end.

malformed_request(Req, State) ->
    {PathInfo, Req1} = cowboy_req:path_info(Req),
    {QsVals, Req2} = cowboy_req:qs_vals(Req1),
    try validate_command(PathInfo, QsVals) of
        {error, V} ->
            lager:error("malformed request ~p", [V]),
            {true, Req2, State};
        M3 ->
            {false, Req2, M3}
    catch
        _:_ ->
            {true, Req2, State}
    end.

to_json(Req, State) ->
    CmdOut = run_command(State),
    case clique_writer:write(CmdOut, "json") of
        {StdOut, []} ->
            {iolist_to_binary(StdOut), Req, undefined};
        {[], StdErr} ->
            {ok, Req1} = cowboy_req:reply(400, [], <<"invalid_request_error">>,
                                          Req),
            {iolist_to_binary(StdErr), Req1, State}
    end.

validate_command(Command, QsVals) ->
    ParamsAndFlags= parse_qs(QsVals),
    M0 = clique_command:match(parse_command(Command)
                              ++ ParamsAndFlags
                              ++ ["--format=json"]),
    M1 = clique_parser:parse(M0),
    M2 = clique_parser:extract_global_flags(M1),
    clique_parser:validate(M2).

parse_qs(QsVals) ->
    parse_qs(QsVals, []).

parse_qs([{<<"--", Flag/binary>>, true}|Rest], Acc) ->
    parse_qs(Rest, ["--" ++ binary_to_list(Flag)|Acc]);
parse_qs([{<<"--", Opt/binary>>, Val}|Rest], Acc) ->
    parse_qs(Rest, ["--" ++ binary_to_list(Opt) ++ "=" ++ binary_to_list(Val)|Acc]);
parse_qs([{Param, Val}|Rest], Acc) ->
    parse_qs(Rest, [binary_to_list(Param) ++ "=" ++ binary_to_list(Val)|Acc]);
parse_qs([], Acc) -> lists:reverse(Acc).

run_command(M3) ->
    {Res, _, _} = clique_command:run(M3),
    Res.

%% Command Aliases
parse_command([<<"cluster">>]) -> parse_command([<<"cluster">>, <<"status">>]);
parse_command([<<"sessions">>]) -> parse_command([<<"session">>, <<"show">>]);
parse_command([<<"sessions">>|Rest]) -> parse_command([<<"session">>|Rest]);
parse_command([<<"listeners">>]) -> parse_command([<<"listener">>, <<"show">>]);
parse_command([<<"listeners">>|Rest]) -> parse_command([<<"listener">>|Rest]);
parse_command([<<"plugins">>]) -> parse_command([<<"plugin">>, <<"show">>]);
parse_command([<<"plugins">>|Rest]) -> parse_command([<<"plugin">>|Rest]);
parse_command(Command) -> ["vmq-admin"| [binary_to_list(C) || C <- Command]].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
routes() ->
    [{"/api/v1/[...]", ?MODULE, []}].

create_api_key() ->
    Chars = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    Size = size(Chars),
    F = fun() -> element(rand:uniform(Size), Chars) end,
    ApiKey = list_to_binary([ F() || _ <- lists:seq(1,32) ]),
    add_api_key(ApiKey),
    ApiKey.

add_api_key(ApiKey) when is_binary(ApiKey) ->
    Keys = vmq_config:get_env(vmq_server, ?ENV_API_KEYS, []),
    Keys1 = lists:delete(ApiKey, Keys),
    vmq_config:set_global_env(vmq_server, ?ENV_API_KEYS, [ApiKey|Keys1], true).

delete_api_key(ApiKey) ->
    case vmq_config:get_env(?ENV_API_KEYS, []) of
        undefined -> ok;
        Keys when is_list(Keys) ->
            vmq_config:set_global_env(vmq_server, ?ENV_API_KEYS, Keys -- [ApiKey], true)
    end,
    ok.

list_api_keys() ->
    vmq_config:get_env(vmq_server, ?ENV_API_KEYS, []).
