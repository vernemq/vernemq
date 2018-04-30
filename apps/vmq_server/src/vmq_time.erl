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

-module(vmq_time).

-export([timestamp/1,
        is_past/1]).

-type timestamp() :: non_neg_integer().
-type duration() :: non_neg_integer().

-spec is_past(timestamp()) -> true | duration().
is_past(ExpiryTs) ->
    Now = timestamp(second),
    case Now >= ExpiryTs of
        true ->
            true;
        false ->
            ExpiryTs - Now
    end.

-spec timestamp(erlang:time_unit()) -> timestamp().
timestamp(millisecond) ->
    %% needed for OTP 18 compatibility
    timestamp(milli_seconds);
timestamp(second) ->
    timestamp(seconds);
timestamp(Unit) ->
    %% We need to get actual time, not plain monotonic time, as
    %% monotonic time isn't portable between systems during queue
    %% migrations.
    erlang:time_offset(Unit) + erlang:monotonic_time(Unit).
