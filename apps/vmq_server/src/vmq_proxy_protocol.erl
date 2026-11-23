%% Copyright 2018-2026 Octavo Labs/VerneMQ (https://vernemq.com/)
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

-module(vmq_proxy_protocol).

-export([
    parse_trusted_proxies/1,
    ranch_trusted_proxies/1
]).

-type trusted_proxies() :: all | [inet:ip_address()].

-spec parse_trusted_proxies(undefined | string() | binary() | trusted_proxies()) ->
    {ok, trusted_proxies()} | {error, any()}.
parse_trusted_proxies(undefined) ->
    {error, proxy_protocol_trusted_proxy_required};
parse_trusted_proxies(all) ->
    {ok, all};
parse_trusted_proxies(Value) when is_binary(Value) ->
    parse_trusted_proxies(binary_to_list(Value));
parse_trusted_proxies([IP | _] = Value) when is_tuple(IP) ->
    case lists:all(fun is_ip_address/1, Value) of
        true -> {ok, lists:usort(Value)};
        false -> {error, {invalid_proxy_protocol_trusted_proxy, Value}}
    end;
parse_trusted_proxies(Value) when is_list(Value) ->
    case string:trim(Value) of
        "" ->
            {error, proxy_protocol_trusted_proxy_required};
        "*" ->
            {ok, all};
        Trimmed ->
            parse_trusted_proxy_entries(string:split(Trimmed, ";", all), [])
    end;
parse_trusted_proxies(Value) ->
    {error, {invalid_proxy_protocol_trusted_proxy, Value}}.

parse_trusted_proxy_entries([Entry0 | Rest], Acc) ->
    Entry = string:trim(Entry0),
    case inet:parse_strict_address(Entry) of
        {ok, IP} -> parse_trusted_proxy_entries(Rest, [IP | Acc]);
        {error, _} -> {error, {invalid_proxy_protocol_trusted_proxy, Entry0}}
    end;
parse_trusted_proxy_entries([], Acc) ->
    {ok, lists:usort(Acc)}.

is_ip_address(IP) when is_tuple(IP), tuple_size(IP) =:= 4 ->
    lists:all(
        fun(Part) -> is_integer(Part) andalso Part >= 0 andalso Part =< 255 end, tuple_to_list(IP)
    );
is_ip_address(IP) when is_tuple(IP), tuple_size(IP) =:= 8 ->
    lists:all(
        fun(Part) -> is_integer(Part) andalso Part >= 0 andalso Part =< 65535 end,
        tuple_to_list(IP)
    );
is_ip_address(_) ->
    false.

-spec ranch_trusted_proxies(trusted_proxies()) -> [ranch:trusted_proxy()].
ranch_trusted_proxies(all) ->
    [{{0, 0, 0, 0}, 0}, {{0, 0, 0, 0, 0, 0, 0, 0}, 0}];
ranch_trusted_proxies(TrustedProxies) ->
    [
        case tuple_size(IP) of
            4 -> {IP, 32};
            8 -> {IP, 128}
        end
     || IP <- TrustedProxies
    ].
