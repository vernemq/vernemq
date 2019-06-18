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
%% @doc vmq_webhooks public API
%% @end
%%%-------------------------------------------------------------------

-module(vmq_webhooks_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([register_webhooks/1]).
-export([apply_config/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    vmq_webhooks_cli:register_cli(),
    vmq_webhooks_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

apply_config(Config) ->
    SchemaFile = [code:priv_dir(vmq_webhooks), "/vmq_webhooks.schema"],
    NewConfig = cuttlefish_unit:generate_config(SchemaFile, conf_parse:parse(Config)),
    OldConfig = application:get_all_env(vmq_webhooks),
    ok = application:set_env(NewConfig),
    OldHooks = proplists:get_value(user_webhooks, OldConfig, []),
    NewHooks = proplists:get_value(user_webhooks,
                                   proplists:get_value(vmq_webhooks, NewConfig, [])),
    ok = deregister_webhooks(OldHooks),
    ok = register_webhooks(NewHooks),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

deregister_webhooks(Webhooks) ->
    [ deregister_webhook(Webhook) || Webhook <- Webhooks ],
    ok.

deregister_webhook({Name, #{hook := HookName, endpoint := Endpoint, options := Opts}}) ->
    case vmq_webhooks_plugin:deregister_endpoint(Endpoint, HookName) of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("failed to deregister the ~p webhook ~p ~p ~p due to ~p",
                        [Name, Endpoint, HookName, Opts, Reason])
    end.


register_webhooks(Webhooks) ->
    [ register_webhook(Webhook) || Webhook <- Webhooks ],
    ok.

register_webhook({Name, #{hook := HookName, endpoint := Endpoint, options := Opts}}) ->
    case vmq_webhooks_plugin:register_endpoint(Endpoint, HookName, Opts) of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("failed to register the ~p webhook ~p ~p ~p due to ~p",
                        [Name, Endpoint, HookName, Opts, Reason])
    end.
