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

%% This module implements X-Forwarded-For support. Currently, only the X-Forwared-For header is supported.
%% X-Forwarded-For expects a list, as in X-Forwarded-For: <client>, <proxy1>, <proxy2>.
%% proxy2 is checked against the configured list of trusted proxies. The trust relationship between proxy2
%% and proxy1 has to be ensured by proxy2.
%% Note that for internal checking we take proxy2 to be the IP of the last proxy, not from its given value 
%% in the XFF header.
%%
%% Not supported at the moment (maybe later):
%% - x-forwarded-client-cert, for CNAME support
%% - Forwarded (RFC 7239)

-module(vmq_proxy_xff).
-include_lib("kernel/include/logger.hrl").

%% API
-export([new_peer/2]).

new_peer(Req, TrustedList) ->
    XFF = header(Req),
    {IP, Port} = cowboy_req:peer(Req),
    {ok} = check_xff_proxy(IP, TrustedList),
    {ok, IP0} = inet:parse_address(extract_xff_origin(XFF)),
    {ok, {IP0, Port}}.

header(Req) ->
    XFFHeader = cowboy_req:header(<<"x-forwarded-for">>, Req, undefined),
    XFFEntries = xff_header_to_list(XFFHeader),
    XFFEntries.

% XFF is a list of IPs. First one, the origin, last one the last proxy
xff_header_to_list(XFFHeader) when is_binary(XFFHeader) ->
    binary:split(XFFHeader, [<<",">>], [global]).

% Check if the contacting proxy is a known one, ie. whether the (current) peer is in TrustedList.
% Note on adapted behaviour: We now do not expect the (last) proxy to add itself to the XFF
check_xff_proxy(IP, TrustedList) ->
    case check_trusted_list(IP, TrustedList) of
        true -> {ok};
        _ -> {error, xff_proxy_addr_not_accepted}
    end.

check_trusted_list(LastProxy, TrustedList) ->
    List = string:tokens(TrustedList, ";"),
    case lists:member(LastProxy, List) of
        true ->
            true;
        _ ->
            ?LOG_ERROR("XFF proxy not in trusted list!"),
            false
    end.

extract_xff_origin(XFFEntries) when is_list(XFFEntries) ->
    binary_to_list(lists:nth(1, XFFEntries)).
