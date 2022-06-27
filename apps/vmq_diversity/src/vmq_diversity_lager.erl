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

-module(vmq_diversity_lager).
-include_lib("luerl/include/luerl.hrl").

-export([install/1]).

install(St) ->
    luerl_emul:alloc_table(table(), St).

table() ->
    [
        {<<"info">>, #erl_func{code = fun info/2}},
        {<<"error">>, #erl_func{code = fun error/2}},
        {<<"debug">>, #erl_func{code = fun debug/2}},
        {<<"warning">>, #erl_func{code = fun warning/2}}
    ].

info([Bin], St) when is_binary(Bin) ->
    lager:info(esc(Bin), []),
    {[], St}.

error([Bin], St) when is_binary(Bin) ->
    lager:error(esc(Bin), []),
    {[], St}.

debug([Bin], St) when is_binary(Bin) ->
    lager:debug(esc(Bin), []),
    {[], St}.

warning([Bin], St) when is_binary(Bin) ->
    lager:warning(esc(Bin), []),
    {[], St}.

esc(Log) when is_binary(Log) ->
    %% escape tildes (~)
    re:replace(Log, <<"~">>, <<"~~">>, [global, {return, binary}]).
