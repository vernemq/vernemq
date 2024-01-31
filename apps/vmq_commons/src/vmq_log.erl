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

-module(vmq_log).
-include_lib("kernel/include/logger.hrl").

-export([set_loglevel/2]).

set_loglevel(Logger, Level) ->
    ?LOG_INFO("Changed log level for Logger ~p to ~p", [Logger, Level]),
    logger:update_handler_config(list_to_atom(Logger), level, list_to_atom(Level)).
