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

-module(vmq_diversity_obfusicator).
-include_lib("luerl/include/luerl.hrl").

-export([install/1, decrypt/2, encrypt/2]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"decrypt">>, #erl_func{code = fun decrypt/2}},
        {<<"encrypt">>, #erl_func{code = fun encrypt/2}}
    ].

decrypt([Bin], St) when is_binary(Bin) ->
    Plain = credentials_obfuscation:decrypt({encrypted, Bin}),
    {NewBin, NewSt} = luerl:encode(Plain, St),
    {[NewBin], NewSt}.

encrypt([Bin], St) when is_binary(Bin) ->
    {encrypted, Enc} = credentials_obfuscation:encrypt(Bin),
    {NewBin, NewSt} = luerl:encode(Enc, St),
    {[NewBin], NewSt}.
