%% Copyright 2019 Octavo Labs AG Zurich Switzerland (https://octavolabs.com)
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
-module(vmq_diversity_crypto).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"hash">>, #erl_func{code = fun hash/2}}
    ].

hash([Alg, Data] = Args, St) when
    is_binary(Alg), is_binary(Data)
->
    try
        {[crypto:hash(binary_to_existing_atom(Alg, utf8), Data)], St}
    catch
        error:badarg ->
            luerl_lib:badarg_error(hash, Args, St)
    end.
