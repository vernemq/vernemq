-module(vmq_json).

-export([decode/1, decode/2, encode/1, encode/2]).

-export_type([json_term/0]).

-define(DEFAULT_ENCODE_OPTS, #{}).
-define(DEFAULT_DECODE_OPTS, #{}).

-type json_term() :: thoas:json_term().

-spec decode(iodata()) -> {ok, json_term()} | {any, any()}.
decode(Data) ->
    decode(Data, ?DEFAULT_DECODE_OPTS).

-spec decode(iodata(), map()) -> {ok, json_term()} | {error, any()}.
decode(Data, Opts) ->
    thoas:decode(Data, Opts).

-spec encode(json_term()) -> iodata().
encode(Term) ->
    encode(Term, ?DEFAULT_ENCODE_OPTS).

-spec encode(json_term(), map()) -> iodata().
encode(Term, Opts) ->
    thoas:encode_to_iodata(Term, Opts).
