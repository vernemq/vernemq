-module(vmq_json).

-export([decode/1, decode/2, encode/1, encode/2, is_json/1]).

-export_type([json_term/0]).

-define(DEFAULT_ENCODE_OPTS, []).
-define(DEFAULT_DECODE_OPTS, []).

-type json_term() :: jsx:json_term().

-spec decode(iodata()) -> json_term() | {incomplete, function()}.
decode(Data) ->
    decode(Data, ?DEFAULT_DECODE_OPTS).

-spec decode(iodata(), list()) -> json_term() | {incomplete, function()}.
decode(Data, Opts) ->
    jsx:decode(Data, Opts).

-spec encode(json_term()) -> iodata() | {incomplete, function()}.
encode(Term) ->
    jsx:encode(Term, ?DEFAULT_ENCODE_OPTS).

-spec encode(json_term(), []) -> iodata() | {incomplete, function()}.
encode(Term, Opts) ->
    jsx:encode(Term, Opts).

-spec is_json(iodata()) -> boolean().
is_json(Data) ->
    jsx:is_json(Data).
