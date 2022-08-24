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

%%%-------------------------------------------------------------------
%% @doc vmq_diversity public API
%% @end
%%%-------------------------------------------------------------------

-module('vmq_diversity_app').

-behaviour(application).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    case vmq_diversity_sup:start_link() of
        {ok, _} = Ret ->
            %% TODO: temporary hack until vmq_plugin is fixed:
            %%     the custom vmq_diversity:start function isn't called
            %%     which results in vmq_diversity_cli:register_cli not
            %%     being triggered.
            %%     As a workaround we check if vmq_plugin is started
            %%     and execute the register_cli here:
            case lists:keymember(vmq_plugin, 1, application:which_applications()) of
                true ->
                    %% started in context of vmq_plugin
                    vmq_diversity_cli:register_cli();
                false ->
                    ignore
            end,

            %% defer the loading of scripts, as the scripts might register plugin
            %% hooks which would end in a deadlock situation, because vmq_diversity
            %% is started as a plugin itself.
            spawn(fun() ->
                DataDir = application:get_env(
                    vmq_diversity,
                    script_dir,
                    code:priv_dir(vmq_diversity) ++ "/../scripts"
                ),
                case filelib:is_dir(DataDir) of
                    true ->
                        lists:foreach(
                            fun(Script) ->
                                load_script(Script)
                            end,
                            filelib:wildcard(DataDir ++ "/*.lua")
                        );
                    false ->
                        lager:warning("can't initialize Lua scripts using ~p", [DataDir])
                end,
                lists:foreach(
                    fun({_Name, Script}) ->
                        lager:info("enable script for ~p", [Script]),
                        load_script(Script)
                    end,
                    application:get_env(vmq_diversity, user_scripts, [])
                ),
                lists:foreach(
                    fun({M, AuthScriptConfig}) ->
                        case proplists:get_value(enabled, AuthScriptConfig, false) of
                            true ->
                                Script = proplists:get_value(file, AuthScriptConfig),
                                lager:info("enable auth script for ~p ~p", [M, Script]),
                                load_script(Script);
                            false ->
                                ignore
                        end
                    end,
                    application:get_env(vmq_diversity, auth_cache, [])
                )
            end),
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
load_script(Script) ->
    case vmq_diversity:load_script(Script) of
        {ok, _Pid} ->
            ok;
        {error, Reason} ->
            lager:error("could not load script ~p due to ~p", [Script, Reason])
    end.
