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

-module(vmq_diversity).

-export([
    start/0,
    load_script/1,
    reload_script/1,
    unload_script/1
]).

start() ->
    {ok, _} = application:ensure_all_started(vmq_diversity),
    vmq_diversity_cli:register_cli(),
    ok.

load_script(Script) when is_list(Script) ->
    case filelib:is_file(Script) of
        true ->
            vmq_diversity_script_sup:start_script(Script);
        false ->
            {error, cant_find_script}
    end.

reload_script(Script) when is_list(Script) ->
    vmq_diversity_script_sup:reload_script(Script).

unload_script(Script) when is_list(Script) ->
    vmq_diversity_script_sup:stop_script(Script).
