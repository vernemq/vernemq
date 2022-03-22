-module(vmq_storage_engine_no_op).

-export([open/2, close/1, write/2, read/2, fold/3, fold/4]).

-record(state, {ref}).

open(_, _) -> {ok, #state{}}.

close(_) -> true.

write(_, _) -> {error, no_op}.

read(_, _) -> not_found.

fold(_, _, Acc) -> Acc.

fold(_, _, Acc, _) -> Acc.
