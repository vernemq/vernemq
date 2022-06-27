%% Copyright 2019 Octavo Labs AG Switzerland (http://octavolabs.com)
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
%%
-module(vmq_pulse).

-export([
    start/0,
    get_cluster_id/0,
    set_cluster_id/1,
    del_cluster_id/0,
    generate_cluster_id/0
]).

start() ->
    application:ensure_all_started(vmq_pulse),
    vmq_pulse_cli:register_cli(),
    ok.

get_cluster_id() ->
    vmq_plugin:only(metadata_get, [{vmq, pulse}, cluster_id]).

set_cluster_id(ClusterId) when is_binary(ClusterId) and (byte_size(ClusterId) == 32) ->
    vmq_plugin:only(metadata_put, [{vmq, pulse}, cluster_id, ClusterId]).

del_cluster_id() ->
    vmq_plugin:only(metadata_delete, [{vmq, pulse}, cluster_id]).

generate_cluster_id() ->
    <<A:4/binary, B:2/binary, C:2/binary, D:2/binary, E:6/binary>> = uuid(),
    iolist_to_binary([hex(A, 8), hex(B, 4), hex(C, 4), hex(D, 4), hex(E, 12)]).

uuid() ->
    Variant = 2,
    Version = 4,
    <<A:48, _:4, B:12, _:2, C:62>> = crypto:strong_rand_bytes(16),
    <<A:48, Version:4, B:12, Variant:2, C:62>>.

hex(Bin, Size) ->
    string:right(integer_to_list(binary:decode_unsigned(Bin, big), 16), Size, $0).
