%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
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

-module(vmq_config_cli).
-export([register_config/0]).

register_config() ->
    case clique_config:load_schema([code:lib_dir()]) of
        {error, schema_files_not_found} ->
            lager:debug("couldn't load cuttlefish schema");
        ok ->
            ConfigKeys =
            ["allow_anonymous",
             "trade_consistency",
             "allow_multiple_sessions",
             "retry_interval",
             "max_client_id_size",
             "persistent_client_expiration",
             "max_inflight_messages",
             "max_queued_messages",
             "message_size_limit",
             "upgrade_outgoing_qos"
            ],
            [clique:register_config([Key], fun register_config_callback/3)
             || Key <- ConfigKeys],
            ok = clique:register_config_whitelist(ConfigKeys)
    end.


register_config_callback([StrKey], _, _) ->
    %% the callback is called, after the application environment is set
    Key = list_to_existing_atom(StrKey),
    {ok, Val} = application:get_env(vmq_server, Key),
    vmq_config:set_env(vmq_server, Key, Val),
    vmq_config:configure_node().
