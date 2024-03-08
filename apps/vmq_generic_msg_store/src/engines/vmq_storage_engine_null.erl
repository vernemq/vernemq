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

-module(vmq_storage_engine_null).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {ref}).

open(_, _) -> 
    {ok, #state{ref=1}}.

close(_) -> true.

write(_, _) -> ok.

read(_, _) -> not_found.

fold(_, _, Acc) -> Acc.

fold(_, _, Acc, _) -> Acc.



