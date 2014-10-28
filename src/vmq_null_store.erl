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

-module(vmq_null_store).
-behaviour(vmq_msg_store).
-export([open/1,
         fold/3,
         delete/2,
         insert/3,
         close/1]).

open(_Args) ->
    {ok, null}.

fold(null, _, Acc) -> Acc.

delete(null, _) -> ok.

insert(null, _, _) -> ok.

close(null) -> ok.

