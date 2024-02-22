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

-module(vmq_auth_apikey).

-export([
    create_api_key/2,
    add_api_key/3,
    delete_api_key/2,
    list_api_keys/0,
    list_api_keys/1,
    scopes/0,
    is_authorized/3
]).

-define(ENV_API_KEYS, http_endpoint_api_keys).

%%
%% API KEY MANAGEMENT
%%

create_api_key(Scope, Expires) ->
    Chars = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    Size = size(Chars),
    F = fun() -> element(rand:uniform(Size), Chars) end,
    MinKeyLength = vmq_config:get_env(vmq_server, min_apikey_length, 0),
    ApiKey = list_to_binary([F() || _ <- lists:seq(1, max(32, MinKeyLength))]),
    case add_api_key(ApiKey, Scope, Expires) of
        ok -> ApiKey;
        Else -> Else
    end.

add_api_key(ApiKey, Scope, Expires) when is_binary(ApiKey) ->
    case {check_key_length(ApiKey), check_key_exists(ApiKey, Scope)} of
        {true, false} ->
            case {check_scope(Scope), check_date(Expires)} of
                {true, true} ->
                    Keys = vmq_config:get_env(vmq_server, ?ENV_API_KEYS, []),
                    Keys1 = lists:delete({ApiKey, Scope}, Keys),
                    ExpiresUTC =
                        case Expires of
                            undefined -> undefined;
                            _ -> parse_date_time_string(Expires)
                        end,
                    vmq_config:set_global_env(
                        vmq_server, ?ENV_API_KEYS, [{ApiKey, Scope, ExpiresUTC} | Keys1], true
                    ),
                    ok;
                {_, false} ->
                    {error, invalid_expiry_date};
                {false, true} ->
                    {error, invalid_scope}
            end;
        {_, true} ->
            {error, key_already_exists};
        {_, _} ->
            {error, invalid_key_length}
    end.

delete_api_key(ApiKey, Scope) ->
    case check_scope(Scope) of
        true ->
            case vmq_config:get_env(?ENV_API_KEYS, []) of
                undefined ->
                    ok;
                Keys when is_list(Keys) ->
                    EntryForDeletion = lists:keyfind(
                        ApiKey, 1, vmq_auth_apikey:list_api_keys(Scope)
                    ),
                    case EntryForDeletion of
                        false ->
                            {error, key_not_found};
                        _ ->
                            vmq_config:set_global_env(
                                vmq_server, ?ENV_API_KEYS, Keys -- [EntryForDeletion], true
                            ),
                            ok
                    end
            end;
        _ ->
            {error, invalid_scope}
    end.

list_api_keys(Scope) ->
    case check_scope(Scope) of
        true ->
            IsInScope = fun({_, KeyScope, _}) -> KeyScope == Scope end,
            lists:filter(IsInScope, vmq_config:get_env(vmq_server, ?ENV_API_KEYS, []));
        _ ->
            {error, invalid_scope}
    end.

list_api_keys() ->
    vmq_config:get_env(vmq_server, ?ENV_API_KEYS, []).

check_api_key(ApiKey, Scope, Req, State) ->
    Res = lists:keyfind(ApiKey, 1, vmq_auth_apikey:list_api_keys(Scope)),
    case Res of
        {Key, _, Expires} ->
            case {Key == ApiKey, not_expired(Expires)} of
                {true, true} -> {true, Req, State};
                _ -> {{false, <<"Authorization realm=\"VerneMQ\"">>}, Req, State}
            end;
        false ->
            {{false, <<"Authorization realm=\"VerneMQ\"">>}, Req, State}
    end.

is_authorized(basic, Req, State, Scope) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, ApiKey, _} ->
            check_api_key(ApiKey, Scope, Req, State);
        _ ->
            {{false, <<"Authorization realm=\"VerneMQ\"">>}, Req, State}
    end;
is_authorized(xapikey, Req, State, Scope) ->
    case cowboy_req:header(<<"x-api-key">>, Req) of
        ApiKey when is_binary(ApiKey) ->
            check_api_key(ApiKey, Scope, Req, State);
        _ ->
            {{false, <<"Authorization realm=\"VerneMQ\"">>}, Req, State}
    end.
is_authorized(Req, State, Scope) ->
    case cowboy_req:parse_header(<<"authorization">>, Req, undefined) of
        undefined -> is_authorized(xapikey, Req, State, Scope);
        _ -> is_authorized(basic, Req, State, Scope)
    end.

%
% internal
%

parse_date_time_string(DateTimeString) ->
    [DateStr, TimeStr] = string:split(DateTimeString, "T"),
    [Year, Month, Day] = [list_to_integer(X) || X <- string:tokens(DateStr, "-")],
    [Hour, Minute, Second] = [list_to_integer(X) || X <- string:tokens(TimeStr, ":")],
    case is_valid_date_time(Year, Month, Day, Hour, Minute, Second) of
        true ->
            DateTimeUTC = calendar:datetime_to_gregorian_seconds(
                hd(
                    calendar:local_time_to_universal_time_dst({
                        {Year, Month, Day}, {Hour, Minute, Second}
                    })
                )
            ),
            DateTimeUTC;
        _ ->
            invalid_datetime_string
    end.

is_valid_date_time(Year, Month, Day, Hour, Minute, Second) ->
    case calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}}) of
        0 -> false;
        DateTimeUTC when is_integer(DateTimeUTC) -> true
    end.

scopes() ->
    ["status", "mgmt", "metrics", "health", "api2", "httppub"].

time_diff_now_secs(ExpiresUTC) ->
    CurrentTime = calendar:now_to_universal_time(erlang:timestamp()),
    calendar:datetime_to_gregorian_seconds(CurrentTime) - ExpiresUTC.

not_expired(undefined) ->
    true;
not_expired(ExpiresUTC) ->
    TimeDiff = time_diff_now_secs(ExpiresUTC),
    TimeDiff < 0.

expired_within_max_range(_, undefined) ->
    true;
expired_within_max_range(ExpiresUTC, MaxRangeInDays) ->
    CurrentTime = calendar:now_to_universal_time(erlang:timestamp()),
    ExpiresUTC < (calendar:datetime_to_gregorian_seconds(CurrentTime) + (MaxRangeInDays * 86400)).

check_date(undefined) ->
    expired_within_max_range(
        undefined, vmq_config:get_env(vmq_server, max_apikey_expiry_days, undefined)
    );
check_date(Expires) ->
    ExpiresUTC = parse_date_time_string(Expires),
    not_expired(ExpiresUTC) and
        expired_within_max_range(
            ExpiresUTC, vmq_config:get_env(vmq_server, max_apikey_expiry_days, undefined)
        ).

check_scope(Scope) ->
    case lists:member(Scope, scopes()) of
        true -> true;
        _ -> false
    end.

check_key_length(ApiKey) ->
    case byte_size(ApiKey) >= vmq_config:get_env(vmq_server, min_apikey_length, 0) of
        true -> true;
        _ -> {error, invalid_key_length}
    end.

check_key_exists(ApiKey, Scope) ->
    is_tuple(lists:keyfind(ApiKey, 1, vmq_auth_apikey:list_api_keys(Scope))).
