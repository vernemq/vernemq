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

-module(vmq_reg_view).
-include("vmq_server.hrl").

-export([fold/5]).

-callback fold(SubscriberId :: subscriber_id(),
               Topic :: topic(),
               FoldFun :: reg_view_fold_fun(),
               Accumulator :: any()

) -> any().
fold(RegView, SubscriberId, Topic, FoldFun, Acc) ->
    RegView:fold(SubscriberId, Topic, FoldFun, Acc).
