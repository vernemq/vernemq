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

-module(vmq_diversity_bcrypt).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"gen_salt">>, #erl_func{code = fun gen_salt/2}},
        {<<"hashpw">>, #erl_func{code = fun hashpw/2}}
    ].

gen_salt(_, St) ->
    {ok, Salt} = bcrypt:gen_salt(),
    {[list_to_binary(Salt)], St}.

hashpw([Pass, Salt], St) when is_binary(Pass) and is_binary(Salt) ->
    {ok, Hash} = bcrypt:hashpw(Pass, Salt),
    {[list_to_binary(Hash)], St}.
