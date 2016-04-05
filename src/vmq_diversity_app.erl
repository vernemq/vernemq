%% Copyright 2016 Erlio GmbH Basel Switzerland (http://erl.io)
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

%%%-------------------------------------------------------------------
%% @doc vmq_diversity public API
%% @end
%%%-------------------------------------------------------------------

-module('vmq_diversity_app').

-behaviour(application).

%% Application callbacks
-export([start/2
        ,stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    case vmq_diversity_sup:start_link() of
        {ok, _} = Ret ->
            DataDir = code:priv_dir(vmq_diversity) ++ "/../scripts",
            case filelib:is_dir(DataDir) of
                true ->
                    lists:foreach(fun(Script) ->
                                          vmq_diversity:load_script(Script)
                                  end, filelib:wildcard(DataDir ++ "/*.lua"));
                false ->
                    lager:warning("Can't initialize Lua Scripts using ~p", [DataDir])
            end,
            Ret;
        E ->
            E
    end.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
