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

-module(vmq_acl_cli).
-export([register/0]).

register() ->
    ConfigKeys =
        [
            "vmq_acl.acl_file",
            "vmq_acl.acl_reload_interval"
        ],
    [
        clique:register_config([Key], fun register_config_callback/2)
     || Key <- ConfigKeys
    ],
    ok = clique:register_config_whitelist(ConfigKeys).

register_config_callback(_, _) ->
    vmq_acl_reloader:change_config_now().
