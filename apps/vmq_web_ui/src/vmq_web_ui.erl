%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(vmq_web_ui).
-include_lib("kernel/include/logger.hrl").

-behaviour(vmq_http_config).

%% cowboy rest handler callbacks
-export([
    init/2,
    allowed_methods/2,
    content_types_accepted/2,
    start/0,
    stop/0,
    routes/0,
    to_html/2,
    process_request/2
]).

start() ->
    {ok, _} = application:ensure_all_started(vmq_http_pub),
    vmq_web_ui_cli:register_cli(),
    ok.

stop() ->
    application:stop(vmq_http_pub).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy REST Handler (GENERIC)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(Req, Opts) ->
    {cowboy_rest, Req, Opts}.

allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"POST">>, <<"GET">>], Req, State}.

to_html(Req, State) ->
    process_request(Req, State).

create_token() ->
    Chars = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    Size = size(Chars),
    F = fun() -> element(rand:uniform(Size), Chars) end,
    list_to_binary([F() || _ <- lists:seq(1, 80)]).

content_types_accepted(Req, State) ->
    %% Return the content types that this resource can accept in a request
    {
        [
            {<<"application/json">>, process_request},
            {<<"text/html">>, process_request},
            {<<"application/octet-stream">>, process_post}
        ],
        Req,
        State
    }.

parse_ln(LongName) ->
    case string:lexemes(LongName, "@") of
        [_A, B | _] ->
            {ok, B};
        _ ->
            {error, long_name}
    end.

make_remote_request(Url, ApiKey) ->
    {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} = httpc:request(
        get, {Url, [{"x-api-key", ApiKey}]}, [], []
    ),
    Body.

process_request(Req, State) ->
    Path = cowboy_req:path(Req),
    case Path of
        <<"/webuiapi/login">> -> login(Req, State);
        <<"/webuiapi/logout">> -> logout(Req, State);
        <<"/webuiapi/v1/options">> -> options(Req, State);
        <<"/webuiapi/v1/read">> -> read(Req, State);
        <<"/webuiapi/v1/write">> -> write(Req, State);
        _ -> forward_request(Req, State)
    end.

invalidate_tokens() ->
    ets:foldl(
        fun(Token, Acc) ->
            case ets:lookup(webuitoken, Token) of
                [{_, TS}] ->
                    case (os:system_time(second) > (TS + (3600 * 48))) of
                        true ->
                            ets:delete(webuitoken, Token);
                        _ ->
                            Acc
                    end;
                _ ->
                    Acc
            end,
            Acc
        end,
        ok,
        webuitoken
    ).

add_token(Token) ->
    ets:insert_new(webuitoken, {Token, os:system_time(second) + 15 * 60}),
    invalidate_tokens().

token_valid(Token) ->
    case ets:lookup(webuitoken, Token) of
        [] ->
            ?LOG_WARNING("Access to webui with an invalid token"),
            false;
        [{_, TS}] ->
            case (os:system_time(second) < TS) of
                true ->
                    true;
                _ ->
                    false
            end;
        _ ->
            ?LOG_WARNING("Access to webui with an invalid token"),
            false
    end.

options(Req, State) ->
    case check_access(Req) of
        true -> do_options(Req, State);
        _ -> invalid_access(Req, State)
    end.

do_options(Req, State) ->
    Config = application:get_env(vmq_web_ui, config, []),
    Config2 = proplists:delete(uiapikey, proplists:delete(uiadminpwd, Config)),
    ConfigAsJson = erlang:iolist_to_binary(vmq_json:encode(Config2)),

    cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"text/json">>},
        ConfigAsJson,
        Req
    ).

logout(Req, _State) ->
    Token = cowboy_req:header(<<"x-token">>, Req, undefined),
    ets:insert(webuitoken, {Token, os:system_time(second) - 1000}).

login(Req, State) ->
    UserName = cowboy_req:header(<<"username">>, Req, undefined),
    Password = cowboy_req:header(<<"password">>, Req, undefined),
    Config = application:get_env(vmq_web_ui, config, []),
    ExpectedUserName = list_to_binary(proplists:get_value(uiadminuser, Config)),
    ExpectedPassword = list_to_binary(proplists:get_value(uiadminpwd, Config)),

    Req2 =
        case {UserName, Password} of
            {ExpectedUserName, ExpectedPassword} ->
                AccessKey = create_token(),
                add_token(AccessKey),

                cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"text/plain">>},
                    AccessKey,
                    Req
                );
            _ ->
                cowboy_req:reply(
                    401,
                    #{<<"content-type">> => <<"text/plain">>},
                    <<"INVALID USERNAME OR PASSWORD">>,
                    Req
                )
        end,
    {stop, Req2, State}.

read(Req, State) ->
    case check_access(Req) of
        true -> do_read(Req, State);
        _ -> invalid_access(Req, State)
    end.

write(Req, State) ->
    case check_access(Req) of
        true -> do_write(Req, State);
        _ -> invalid_access(Req, State)
    end.

do_read(Req, State) ->
    Config = application:get_env(vmq_web_ui, config, []),
    AllowRead = proplists:get_value(uifileaccessallowread, Config, false),
    case AllowRead of
        true ->
            Params = cowboy_req:parse_qs(Req),
            Ft = proplists:get_value(<<"file">>, Params, undefined),
            ?LOG_INFO("Loading tyoe ~p~n", [Ft]),

            {ok, FileName} =
                case Ft of
                    <<"vmq_passwd">> -> {ok, application:get_env(vmq_passwd, file)};
                    <<"vmq_acl">> -> {ok, application:get_env(vmq_acl, file)};
                    <<"vmq_config">> -> {ok, "./etc/vernemq.conf"};
                    _ -> {error, invalid_request}
                end,
            ?LOG_INFO("Loading file ~p~n", [FileName]),
            {ok, Data} = file:read_file(FileName),

            Req2 = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"text/plain">>},
                Data,
                Req
            ),
            {stop, Req2, State};
        _ ->
            Req2 = cowboy_req:reply(
                403,
                #{<<"content-type">> => <<"text/plain">>},
                <<"Not enabled">>,
                Req
            ),
            {stop, Req2, State}
    end.

do_write(Req, State) ->
    Config = application:get_env(vmq_web_ui, config, []),
    AllowWrite = proplists:get_value(uifileaccessallowwrite, Config, false),
    case AllowWrite of
        true ->
            Params = cowboy_req:parse_qs(Req),
            Ft = proplists:get_value(<<"file">>, Params, undefined),
            ?LOG_INFO("Writing type ~p~n", [Ft]),

            {ok, FileName} =
                case Ft of
                    <<"vmq_passwd">> -> {ok, application:get_env(vmq_passwd, file)};
                    <<"vmq_acl">> -> {ok, application:get_env(vmq_acl, file)};
                    <<"vmq_config">> -> {ok, "./etc/vernemq.conf"};
                    _ -> {error, invalid_request}
                end,

            ?LOG_INFO("Write is currently not implemented.");
        _ ->
            ?LOG_WARNING("Write attempt by UI, but write is disabled.")
    end,
    ok.

check_access(Req) ->
    Token = cowboy_req:header(<<"x-token">>, Req, undefined),
    case token_valid(Token) of
        true -> true;
        _ -> false
    end.

do_forward_request(Req, State) ->
    Path = cowboy_req:path(Req),
    Tokens = string:lexemes(Path, "/"),
    Fwd = lists:nth(4, Tokens),
    VerneMQNode =
        case Fwd of
            <<"self">> -> atom_to_binary(node());
            _ -> Fwd
        end,

    case lists:member(binary_to_atom(VerneMQNode), vmq_cluster:nodes()) of
        true ->
            {ok, LongName} = parse_ln(VerneMQNode),
            API = lists:sublist(Tokens, 5, length(Tokens)),
            APICall = list_to_binary(lists:join("/", API)),
            Config = application:get_env(vmq_web_ui, config, []),
            URLSchema = proplists:get_value(uiapischeme, Config),
            URLPort = proplists:get_value(uiapiport, Config),
            APIKey = proplists:get_value(uiapikey, Config),
            FinalURL = list_to_binary([
                URLSchema, "://", LongName, ":", URLPort, "/", APICall, "?", cowboy_req:qs(Req)
            ]),
            Body = make_remote_request(FinalURL, APIKey),

            Req2 = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"text/plain">>},
                Body,
                Req
            ),
            {stop, Req2, State};
        false ->
            logout(Req, State),
            ?LOG_ERROR("WebUI tried to access an invalid Node ~p. This should be investigated.", [
                VerneMQNode
            ]),
            Req2 = cowboy_req:reply(
                403,
                #{<<"content-type">> => <<"text/plain">>},
                <<"Invalid long name">>,
                Req
            ),
            {stop, Req2, State}
    end.

invalid_access(Req, State) ->
    Req2 = cowboy_req:reply(
        401,
        #{<<"content-type">> => <<"text/plain">>},
        <<"INVALID AUTH KEY">>,
        Req
    ),
    {stop, Req2, State}.

forward_request(Req, State) ->
    case check_access(Req) of
        true -> do_forward_request(Req, State);
        _ -> invalid_access(Req, State)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
routes() ->
    ets:new(webuitoken, [named_table, public, set]),
    [
        {"/webui", cowboy_static, {priv_file, vmq_web_ui, "www/index.html"}},
        {"/webui/[...]", cowboy_static, {priv_dir, vmq_web_ui, "www"}},
        {"/assets/[...]", cowboy_static, {priv_dir, vmq_web_ui, "www/assets"}},
        {"/webuiapi/login", ?MODULE, []},
        {"/webuiapi/logout", ?MODULE, []},
        {"/webuiapi/v1/[...]", ?MODULE, []}
    ].
