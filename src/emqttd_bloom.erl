%% @doc Implementation of the Bloom filter data structure.
%% @reference [http://en.wikipedia.org/wiki/Bloom_filter]

-module(emqttd_bloom).
-export([new/1, new/2, is_bloom/1, is_element/2, is_subset/2, equal/2, union/2, add_element/2, count/1]).
-import(math, [log/1, pow/2]).
-import(erlang, [phash2/2]).

-record(bloom, {
        m      = 0,       % The size of the bitmap in bits.
        bitmap = <<>>,    % The bitmap.
        k      = 0,       % The number of hashes.
        n      = 0,       % The maximum number of keys.
        keys   = 0        % The current number of keys.
        }).

%% @spec new(capacity) -> bloom()
%% @equiv new(capacity, 0.001)
new(N) -> new(N, 0.001).

%% @spec new(integer(), float()) -> bloom()
%% @doc Creates a new Bloom filter, given a maximum number of keys and a
%%     false-positive error rate.
new(N, E) when N > 0, is_float(E), E > 0, E =< 1 ->
    {M, K} = calc_least_bits(N, E),
    #bloom{m=M, bitmap = <<0:((M+7) div 8 * 8)>>, k=K, n=N}.

%% @spec is_bloom(bloom()) -> bool()
%% @doc Determines if the given argument is a bloom record.
is_bloom(#bloom{}) -> true;
is_bloom(_) -> false.

%% @spec is_element(string(), bloom()) -> bool()
%% @doc Determines if the key is (probably) an element of the filter.
is_element(Key, B) -> is_element(Key, B, calc_idxs(Key, B)).
is_element(_, _, []) -> true;
is_element(Key, B, [Idx | T]) ->
    ByteIdx = Idx div 8,
    <<_:ByteIdx/binary, Byte:8, _/binary>> = B#bloom.bitmap,
    Mask = 1 bsl (Idx rem 8),
    case 0 =/= Byte band Mask of
        true -> is_element(Key, B, T);
        false -> false
    end.

is_subset(BS, B) ->
    Idxs = [I || I <- lists:seq(0, byte_size(BS#bloom.bitmap)-1),binary:at(BS#bloom.bitmap, I) =/= 0],
    is_subset(BS, B, Idxs).

is_subset(_, _, []) -> true;
is_subset(BS, B, [Idx | T]) ->
    <<_:Idx/binary, Mask:8, _/binary>> = B#bloom.bitmap,
    <<_:Idx/binary, Byte:8, _/binary>> = BS#bloom.bitmap,
    case 0 =/= Byte band Mask of
        true -> is_subset(BS, B, T);
        false -> false
    end.

union(A, B) ->
    AIdxs = [I || I <- lists:seq(0, byte_size(A#bloom.bitmap)-1),binary:at(A#bloom.bitmap, I) =/= 0],
    BIdxs = [I || I <- lists:seq(0, byte_size(B#bloom.bitmap)-1),binary:at(B#bloom.bitmap, I) =/= 0],
    unify(unify(A, B, AIdxs), B, BIdxs).

unify(A, _, []) -> A;
unify(A, B, [Idx|T]) ->
    <<PA:Idx/binary, ByteA:8, R/binary>> = A#bloom.bitmap,
    <<_:Idx/binary, ByteB:8, _/binary>> = B#bloom.bitmap,
    Byte = ByteA bor ByteB,
    unify(A#bloom{bitmap= <<PA/binary, Byte:8, R/binary>>}, B, T).

count(B) ->
    B#bloom.keys.

equal(A, B) ->
    A#bloom.bitmap =:= B#bloom.bitmap.

%% @spec add_element(string(), bloom()) -> bloom()
%% @doc Adds the key to the filter.
add_element(Key, #bloom{keys=Keys, n=N, bitmap=Bitmap} = B) when Keys < N ->
    Idxs = calc_idxs(Key, B),
    Bitmap0 = set_bits(Bitmap, Idxs),
    case Bitmap0 == Bitmap of
        true -> B;    % Don't increment key count for duplicates.
        false -> B#bloom{bitmap=Bitmap0, keys=Keys+1}
    end.

set_bits(Bin, []) -> Bin;
set_bits(Bin, [Idx | Idxs]) ->
    ByteIdx = Idx div 8,
    <<Pre:ByteIdx/binary, Byte:8, Post/binary>> = Bin,
    Mask = 1 bsl (Idx rem 8),
    Byte0 = Byte bor Mask,
    set_bits(<<Pre/binary, Byte0:8, Post/binary>>, Idxs).

% Find the optimal bitmap size and number of hashes.
calc_least_bits(N, E) -> calc_least_bits(N, E, 1, 0, 0).
calc_least_bits(N, E, K, MinM, BestK) ->
    M = -1 * K * N / log(1 - pow(E, 1/K)),
    {CurM, CurK} = if M < MinM -> {M, K}; true -> {MinM, BestK} end,
    case K of
        1 -> calc_least_bits(N, E, K+1, M, K);
        100 -> {trunc(CurM)+1, CurK};
        _ -> calc_least_bits(N, E, K+1, CurM, CurK)
    end.

% This uses the "enhanced double hashing" algorithm.
% Todo: handle case of m > 2^32.
calc_idxs(Key, #bloom{m=M, k=K}) ->
    X = phash2(Key, M),
    Y = phash2({"salt", Key}, M),
    calc_idxs(M, K - 1, X, Y, [X]).
calc_idxs(_, 0, _, _, Acc) -> Acc;
calc_idxs(M, I, X, Y, Acc) ->
    Xi = (X+Y) rem M,
    Yi = (Y+I) rem M,
    calc_idxs(M, I-1, Xi, Yi, [Xi | Acc]).


