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
-module(vmq_schema_util).

-export([file_exists/1, file_is_readable/1, file_is_pem_content/1]).

-include_lib("kernel/include/file.hrl").

file_exists(Filename) ->
    case file:read_file_info(Filename) of
        {error, enonent} ->
            false;
        _ ->
            true
    end.

file_is_readable(Filename) ->
    case file:read_file_info(Filename) of
        {ok, #file_info{access = Access}} when Access =:= read; Access =:= read_write ->
            %% we can read the file
            true;
        _ ->
            false
    end.

%% Is the filename PEM-formatted with one or more entries
file_is_pem_content(Filename) ->
    case file:read_file(Filename) of
        {ok, FileContents} ->
            case catch public_key:pem_decode(FileContents) of
                [] -> false;
                [_ | _] -> true
            end;
        _ ->
            false
    end.
