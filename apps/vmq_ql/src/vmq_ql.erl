-module(vmq_ql).
-export([parse/1,file/1]).
-define(p_anything,true).
-define(p_charclass,true).
-define(p_choose,true).
-define(p_label,true).
-define(p_not,true).
-define(p_one_or_more,true).
-define(p_optional,true).
-define(p_scan,true).
-define(p_seq,true).
-define(p_string,true).
-define(p_zero_or_more,true).



-spec file(file:name()) -> any().
file(Filename) -> case file:read_file(Filename) of {ok,Bin} -> parse(Bin); Err -> Err end.

-spec parse(binary() | list()) -> any().
parse(List) when is_list(List) -> parse(unicode:characters_to_binary(List));
parse(Input) when is_binary(Input) ->
  _ = setup_memo(),
  Result = case 'start'(Input,{{line,1},{column,1}}) of
             {AST, <<>>, _Index} -> AST;
             Any -> Any
           end,
  release_memo(), Result.

-spec 'start'(input(), index()) -> parse_result().
'start'(Input, Index) ->
  p(Input, Index, 'start', fun(I,D) -> (fun 'stmt'/2)(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'stmt'(input(), index()) -> parse_result().
'stmt'(Input, Index) ->
  p(Input, Index, 'stmt', fun(I,D) -> (fun 'select_stmt'/2)(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'select_stmt'(input(), index()) -> parse_result().
'select_stmt'(Input, Index) ->
  p(Input, Index, 'select_stmt', fun(I,D) -> (p_seq([fun '_'/2, fun 'select_token'/2, fun '_'/2, p_label('x', fun 'select_field'/2), p_label('xs', p_zero_or_more(fun 'select_field_rest'/2)), fun '_'/2, fun 'from_token'/2, fun '_'/2, p_label('from', fun 'identifier'/2), fun '_'/2, p_label('where', p_optional(fun 'where_expr'/2)), fun '_'/2, p_label('orderby', p_optional(fun 'orderby_expr'/2)), fun '_'/2, p_label('limit', p_optional(fun 'limit_expr'/2)), p_optional(p_seq([fun '_'/2, fun 'limit_token'/2, fun '_'/2, p_label('limit', fun 'pos_integer'/2)]))]))(I,D) end, fun(Node, _Idx) ->
[{type, "SELECT"}, 
 {fields, [proplists:get_value(x, Node)] ++ proplists:get_value(xs, Node, [])},
 {from, proplists:get_value(from, Node)},
 {where, proplists:get_value(where, Node)},
 {orderby, proplists:get_value(orderby, Node)},
 {limit, proplists:get_value(limit, Node)}]
 end).

-spec 'select_field'(input(), index()) -> parse_result().
'select_field'(Input, Index) ->
  p(Input, Index, 'select_field', fun(I,D) -> (p_choose([fun 'identifier'/2, fun 'select_wildcard'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'select_field_rest'(input(), index()) -> parse_result().
'select_field_rest'(Input, Index) ->
  p(Input, Index, 'select_field_rest', fun(I,D) -> (p_seq([fun '_'/2, fun 'separator_token'/2, fun '_'/2, p_label('s', fun 'select_field'/2)]))(I,D) end, fun(Node, _Idx) ->proplists:get_value(s, Node) end).

-spec 'select_wildcard'(input(), index()) -> parse_result().
'select_wildcard'(Input, Index) ->
  p(Input, Index, 'select_wildcard', fun(I,D) -> (p_string(<<"*">>))(I,D) end, fun(_Node, _Idx) ->all end).

-spec 'orderby_expr'(input(), index()) -> parse_result().
'orderby_expr'(Input, Index) ->
  p(Input, Index, 'orderby_expr', fun(I,D) -> (p_seq([fun 'orderby_token'/2, fun '_'/2, p_label('x', fun 'orderby_field'/2)]))(I,D) end, fun(Node, _Idx) ->
[proplists:get_value(x, Node)]
 end).

-spec 'orderby_field'(input(), index()) -> parse_result().
'orderby_field'(Input, Index) ->
  p(Input, Index, 'orderby_field', fun(I,D) -> (fun 'identifier'/2)(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'limit_expr'(input(), index()) -> parse_result().
'limit_expr'(Input, Index) ->
  p(Input, Index, 'limit_expr', fun(I,D) -> (p_seq([fun 'limit_token'/2, fun '_'/2, p_label('x', fun 'pos_integer'/2), fun '_'/2]))(I,D) end, fun(Node, _Idx) ->
list_to_integer(binary_to_list(iolist_to_binary(proplists:get_value(x, Node))))
 end).

-spec 'where_expr'(input(), index()) -> parse_result().
'where_expr'(Input, Index) ->
  p(Input, Index, 'where_expr', fun(I,D) -> (p_seq([fun 'where_token'/2, p_label('x', p_one_or_more(fun 'logic_expr'/2))]))(I,D) end, fun(Node, _Idx) ->
proplists:get_value(x, Node)
 end).

-spec 'logic_expr'(input(), index()) -> parse_result().
'logic_expr'(Input, Index) ->
  p(Input, Index, 'logic_expr', fun(I,D) -> (p_choose([p_seq([fun '_'/2, p_string(<<"(">>), fun '_'/2, p_label('x', p_one_or_more(fun 'logic_expr'/2)), fun '_'/2, p_string(<<")">>), fun '_'/2]), p_seq([fun '_'/2, p_label('left', fun 'expr'/2), fun '_'/2, p_label('op', fun 'operator'/2), fun '_'/2, p_label('right', p_choose([fun 'logic_expr'/2, fun 'expr'/2])), fun '_'/2])]))(I,D) end, fun(Node, _Idx) ->
case proplists:get_value(x, Node) of
  undefined -> 
    {op,
     proplists:get_value(left, Node),
     proplists:get_value(op, Node),
     proplists:get_value(right, Node)};
  X ->
    [X] ++ proplists:get_value(xs, Node, [])
end
 end).

-spec 'operator'(input(), index()) -> parse_result().
'operator'(Input, Index) ->
  p(Input, Index, 'operator', fun(I,D) -> (p_choose([p_string(<<"!=">>), p_string(<<"=<">>), p_string(<<">=">>), p_string(<<"=">>), p_string(<<">">>), p_string(<<"<">>), fun 'or_token'/2, fun 'and_token'/2, fun 'like_token'/2]))(I,D) end, fun(Node, _Idx) ->
case Node of
  <<"!=">> -> 'not_equals';
  <<"=">> -> 'equals';
  <<"<">> -> 'lesser';
  <<"=<">> -> 'lesser_equals';
  <<">">> -> 'greater';
  <<">=">> -> 'greater_equals';
  [<<"OR">>, []] -> 'or';
  [<<"AND">>, []] -> 'and';
  [<<"LIKE">>, []] -> 'like'
end
 end).

-spec 'expr'(input(), index()) -> parse_result().
'expr'(Input, Index) ->
  p(Input, Index, 'expr', fun(I,D) -> (p_choose([fun 'number'/2, fun 'bool'/2, fun 'identifier'/2, fun 'string'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'number'(input(), index()) -> parse_result().
'number'(Input, Index) ->
  p(Input, Index, 'number', fun(I,D) -> (p_seq([fun 'integer'/2, p_optional(fun 'frac'/2), p_optional(fun 'exp'/2)]))(I,D) end, fun(Node, _Idx) ->
case Node of
  [Int, [], []] -> list_to_integer(binary_to_list(iolist_to_binary(Int)));
  [Int, Frac, []] -> list_to_float(binary_to_list(iolist_to_binary([Int, Frac])));
  [Int, _, Exp] -> list_to_float(binary_to_list(iolist_to_binary([Int, ".0", Exp])));
  _ -> list_to_float(binary_to_list(iolist_to_binary(Node)))
end
 end).

-spec 'pos_integer'(input(), index()) -> parse_result().
'pos_integer'(Input, Index) ->
  p(Input, Index, 'pos_integer', fun(I,D) -> (p_choose([p_seq([fun 'non_zero_digit'/2, p_one_or_more(fun 'digit'/2)]), p_one_or_more(fun 'digit'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'integer'(input(), index()) -> parse_result().
'integer'(Input, Index) ->
  p(Input, Index, 'integer', fun(I,D) -> (p_choose([p_seq([p_optional(p_string(<<"-">>)), p_seq([fun 'non_zero_digit'/2, p_one_or_more(fun 'digit'/2)])]), p_one_or_more(fun 'digit'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'frac'(input(), index()) -> parse_result().
'frac'(Input, Index) ->
  p(Input, Index, 'frac', fun(I,D) -> (p_seq([p_string(<<".">>), p_one_or_more(fun 'digit'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'exp'(input(), index()) -> parse_result().
'exp'(Input, Index) ->
  p(Input, Index, 'exp', fun(I,D) -> (p_seq([fun 'e'/2, p_one_or_more(fun 'digit'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'e'(input(), index()) -> parse_result().
'e'(Input, Index) ->
  p(Input, Index, 'e', fun(I,D) -> (p_seq([p_charclass(<<"[eE]">>), p_optional(p_choose([p_string(<<"+">>), p_string(<<"-">>)]))]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'non_zero_digit'(input(), index()) -> parse_result().
'non_zero_digit'(Input, Index) ->
  p(Input, Index, 'non_zero_digit', fun(I,D) -> (p_charclass(<<"[1-9]">>))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'digit'(input(), index()) -> parse_result().
'digit'(Input, Index) ->
  p(Input, Index, 'digit', fun(I,D) -> (p_charclass(<<"[0-9]">>))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'bool'(input(), index()) -> parse_result().
'bool'(Input, Index) ->
  p(Input, Index, 'bool', fun(I,D) -> (p_choose([fun 'true'/2, fun 'false'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'true'(input(), index()) -> parse_result().
'true'(Input, Index) ->
  p(Input, Index, 'true', fun(I,D) -> (p_string(<<"true">>))(I,D) end, fun(_Node, _Idx) ->true end).

-spec 'false'(input(), index()) -> parse_result().
'false'(Input, Index) ->
  p(Input, Index, 'false', fun(I,D) -> (p_string(<<"false">>))(I,D) end, fun(_Node, _Idx) ->false end).

-spec 'string'(input(), index()) -> parse_result().
'string'(Input, Index) ->
  p(Input, Index, 'string', fun(I,D) -> (p_seq([p_string(<<"\"">>), p_label('chars', p_zero_or_more(p_seq([p_not(p_string(<<"\"">>)), p_choose([p_string(<<"\\\\">>), p_string(<<"\\\"">>), p_anything()])]))), p_string(<<"\"">>)]))(I,D) end, fun(Node, _Idx) ->iolist_to_binary(proplists:get_value(chars, Node)) end).

-spec 'select_token'(input(), index()) -> parse_result().
'select_token'(Input, Index) ->
  p(Input, Index, 'select_token', fun(I,D) -> (p_seq([p_string(<<"SELECT">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'separator_token'(input(), index()) -> parse_result().
'separator_token'(Input, Index) ->
  p(Input, Index, 'separator_token', fun(I,D) -> (p_string(<<",">>))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'from_token'(input(), index()) -> parse_result().
'from_token'(Input, Index) ->
  p(Input, Index, 'from_token', fun(I,D) -> (p_seq([p_string(<<"FROM">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'where_token'(input(), index()) -> parse_result().
'where_token'(Input, Index) ->
  p(Input, Index, 'where_token', fun(I,D) -> (p_seq([p_string(<<"WHERE">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'like_token'(input(), index()) -> parse_result().
'like_token'(Input, Index) ->
  p(Input, Index, 'like_token', fun(I,D) -> (p_seq([p_string(<<"LIKE">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'or_token'(input(), index()) -> parse_result().
'or_token'(Input, Index) ->
  p(Input, Index, 'or_token', fun(I,D) -> (p_seq([p_string(<<"OR">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'and_token'(input(), index()) -> parse_result().
'and_token'(Input, Index) ->
  p(Input, Index, 'and_token', fun(I,D) -> (p_seq([p_string(<<"AND">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'orderby_token'(input(), index()) -> parse_result().
'orderby_token'(Input, Index) ->
  p(Input, Index, 'orderby_token', fun(I,D) -> (p_seq([p_string(<<"ORDER\sBY">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'limit_token'(input(), index()) -> parse_result().
'limit_token'(Input, Index) ->
  p(Input, Index, 'limit_token', fun(I,D) -> (p_seq([p_string(<<"LIMIT">>), p_not(fun 'ident_rest'/2)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'identifier'(input(), index()) -> parse_result().
'identifier'(Input, Index) ->
  p(Input, Index, 'identifier', fun(I,D) -> (p_seq([p_label('x', fun 'ident_start'/2), p_label('xs', p_zero_or_more(fun 'ident_rest'/2))]))(I,D) end, fun(Node, _Idx) ->
binary_to_existing_atom(iolist_to_binary([
    proplists:get_value(x, Node),
    proplists:get_value(xs, Node, [])
]), utf8)
 end).

-spec 'ident_start'(input(), index()) -> parse_result().
'ident_start'(Input, Index) ->
  p(Input, Index, 'ident_start', fun(I,D) -> (p_charclass(<<"[a-z_]">>))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'ident_rest'(input(), index()) -> parse_result().
'ident_rest'(Input, Index) ->
  p(Input, Index, 'ident_rest', fun(I,D) -> (p_charclass(<<"[a-z0-9_]">>))(I,D) end, fun(Node, _Idx) ->Node end).

-spec '_'(input(), index()) -> parse_result().
'_'(Input, Index) ->
  p(Input, Index, '_', fun(I,D) -> (p_zero_or_more(p_choose([fun 'newline'/2, fun 'whitespace'/2])))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'newline'(input(), index()) -> parse_result().
'newline'(Input, Index) ->
  p(Input, Index, 'newline', fun(I,D) -> (p_choose([p_string(<<"\r\n">>), p_string(<<"\r">>), p_string(<<"\n">>), p_string(<<"\u2028">>), p_string(<<"\u2029">>)]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'whitespace'(input(), index()) -> parse_result().
'whitespace'(Input, Index) ->
  p(Input, Index, 'whitespace', fun(I,D) -> (p_choose([p_string(<<"\s">>), p_string(<<"\t">>), p_string(<<"\v">>), p_string(<<"\f">>)]))(I,D) end, fun(Node, _Idx) ->Node end).



-file("peg_includes.hrl", 1).
-type index() :: {{line, pos_integer()}, {column, pos_integer()}}.
-type input() :: binary().
-type parse_failure() :: {fail, term()}.
-type parse_success() :: {term(), input(), index()}.
-type parse_result() :: parse_failure() | parse_success().
-type parse_fun() :: fun((input(), index()) -> parse_result()).
-type xform_fun() :: fun((input(), index()) -> term()).

-spec p(input(), index(), atom(), parse_fun(), xform_fun()) -> parse_result().
p(Inp, StartIndex, Name, ParseFun, TransformFun) ->
  case get_memo(StartIndex, Name) of      % See if the current reduction is memoized
    {ok, Memo} -> %Memo;                     % If it is, return the stored result
      Memo;
    _ ->                                        % If not, attempt to parse
      Result = case ParseFun(Inp, StartIndex) of
        {fail,_} = Failure ->                       % If it fails, memoize the failure
          Failure;
        {Match, InpRem, NewIndex} ->               % If it passes, transform and memoize the result.
          Transformed = TransformFun(Match, StartIndex),
          {Transformed, InpRem, NewIndex}
      end,
      memoize(StartIndex, Name, Result),
      Result
  end.

-spec setup_memo() -> ets:tid().
setup_memo() ->
  put({parse_memo_table, ?MODULE}, ets:new(?MODULE, [set])).

-spec release_memo() -> true.
release_memo() ->
  ets:delete(memo_table_name()).

-spec memoize(index(), atom(), parse_result()) -> true.
memoize(Index, Name, Result) ->
  Memo = case ets:lookup(memo_table_name(), Index) of
              [] -> [];
              [{Index, Plist}] -> Plist
         end,
  ets:insert(memo_table_name(), {Index, [{Name, Result}|Memo]}).

-spec get_memo(index(), atom()) -> {ok, term()} | {error, not_found}.
get_memo(Index, Name) ->
  case ets:lookup(memo_table_name(), Index) of
    [] -> {error, not_found};
    [{Index, Plist}] ->
      case proplists:lookup(Name, Plist) of
        {Name, Result}  -> {ok, Result};
        _  -> {error, not_found}
      end
    end.

-spec memo_table_name() -> ets:tid().
memo_table_name() ->
    get({parse_memo_table, ?MODULE}).

-ifdef(p_eof).
-spec p_eof() -> parse_fun().
p_eof() ->
  fun(<<>>, Index) -> {eof, [], Index};
     (_, Index) -> {fail, {expected, eof, Index}} end.
-endif.

-ifdef(p_optional).
-spec p_optional(parse_fun()) -> parse_fun().
p_optional(P) ->
  fun(Input, Index) ->
      case P(Input, Index) of
        {fail,_} -> {[], Input, Index};
        {_, _, _} = Success -> Success
      end
  end.
-endif.

-ifdef(p_not).
-spec p_not(parse_fun()) -> parse_fun().
p_not(P) ->
  fun(Input, Index)->
      case P(Input,Index) of
        {fail,_} ->
          {[], Input, Index};
        {Result, _, _} -> {fail, {expected, {no_match, Result},Index}}
      end
  end.
-endif.

-ifdef(p_assert).
-spec p_assert(parse_fun()) -> parse_fun().
p_assert(P) ->
  fun(Input,Index) ->
      case P(Input,Index) of
        {fail,_} = Failure-> Failure;
        _ -> {[], Input, Index}
      end
  end.
-endif.

-ifdef(p_seq).
-spec p_seq([parse_fun()]) -> parse_fun().
p_seq(P) ->
  fun(Input, Index) ->
      p_all(P, Input, Index, [])
  end.

-spec p_all([parse_fun()], input(), index(), [term()]) -> parse_result().
p_all([], Inp, Index, Accum ) -> {lists:reverse( Accum ), Inp, Index};
p_all([P|Parsers], Inp, Index, Accum) ->
  case P(Inp, Index) of
    {fail, _} = Failure -> Failure;
    {Result, InpRem, NewIndex} -> p_all(Parsers, InpRem, NewIndex, [Result|Accum])
  end.
-endif.

-ifdef(p_choose).
-spec p_choose([parse_fun()]) -> parse_fun().
p_choose(Parsers) ->
  fun(Input, Index) ->
      p_attempt(Parsers, Input, Index, none)
  end.

-spec p_attempt([parse_fun()], input(), index(), none | parse_failure()) -> parse_result().
p_attempt([], _Input, _Index, Failure) -> Failure;
p_attempt([P|Parsers], Input, Index, FirstFailure)->
  case P(Input, Index) of
    {fail, _} = Failure ->
      case FirstFailure of
        none -> p_attempt(Parsers, Input, Index, Failure);
        _ -> p_attempt(Parsers, Input, Index, FirstFailure)
      end;
    Result -> Result
  end.
-endif.

-ifdef(p_zero_or_more).
-spec p_zero_or_more(parse_fun()) -> parse_fun().
p_zero_or_more(P) ->
  fun(Input, Index) ->
      p_scan(P, Input, Index, [])
  end.
-endif.

-ifdef(p_one_or_more).
-spec p_one_or_more(parse_fun()) -> parse_fun().
p_one_or_more(P) ->
  fun(Input, Index)->
      Result = p_scan(P, Input, Index, []),
      case Result of
        {[_|_], _, _} ->
          Result;
        _ ->
          {fail, {expected, Failure, _}} = P(Input,Index),
          {fail, {expected, {at_least_one, Failure}, Index}}
      end
  end.
-endif.

-ifdef(p_label).
-spec p_label(atom(), parse_fun()) -> parse_fun().
p_label(Tag, P) ->
  fun(Input, Index) ->
      case P(Input, Index) of
        {fail,_} = Failure ->
           Failure;
        {Result, InpRem, NewIndex} ->
          {{Tag, Result}, InpRem, NewIndex}
      end
  end.
-endif.

-ifdef(p_scan).
-spec p_scan(parse_fun(), input(), index(), [term()]) -> {[term()], input(), index()}.
p_scan(_, <<>>, Index, Accum) -> {lists:reverse(Accum), <<>>, Index};
p_scan(P, Inp, Index, Accum) ->
  case P(Inp, Index) of
    {fail,_} -> {lists:reverse(Accum), Inp, Index};
    {Result, InpRem, NewIndex} -> p_scan(P, InpRem, NewIndex, [Result | Accum])
  end.
-endif.

-ifdef(p_string).
-spec p_string(binary()) -> parse_fun().
p_string(S) ->
    Length = erlang:byte_size(S),
    fun(Input, Index) ->
      try
          <<S:Length/binary, Rest/binary>> = Input,
          {S, Rest, p_advance_index(S, Index)}
      catch
          error:{badmatch,_} -> {fail, {expected, {string, S}, Index}}
      end
    end.
-endif.

-ifdef(p_anything).
-spec p_anything() -> parse_fun().
p_anything() ->
  fun(<<>>, Index) -> {fail, {expected, any_character, Index}};
     (Input, Index) when is_binary(Input) ->
          <<C/utf8, Rest/binary>> = Input,
          {<<C/utf8>>, Rest, p_advance_index(<<C/utf8>>, Index)}
  end.
-endif.

-ifdef(p_charclass).
-spec p_charclass(string() | binary()) -> parse_fun().
p_charclass(Class) ->
    {ok, RE} = re:compile(Class, [unicode, dotall]),
    fun(Inp, Index) ->
            case re:run(Inp, RE, [anchored]) of
                {match, [{0, Length}|_]} ->
                    {Head, Tail} = erlang:split_binary(Inp, Length),
                    {Head, Tail, p_advance_index(Head, Index)};
                _ -> {fail, {expected, {character_class, binary_to_list(Class)}, Index}}
            end
    end.
-endif.

-ifdef(p_regexp).
-spec p_regexp(binary()) -> parse_fun().
p_regexp(Regexp) ->
    {ok, RE} = re:compile(Regexp, [unicode, dotall, anchored]),
    fun(Inp, Index) ->
        case re:run(Inp, RE) of
            {match, [{0, Length}|_]} ->
                {Head, Tail} = erlang:split_binary(Inp, Length),
                {Head, Tail, p_advance_index(Head, Index)};
            _ -> {fail, {expected, {regexp, binary_to_list(Regexp)}, Index}}
        end
    end.
-endif.

-ifdef(line).
-spec line(index() | term()) -> pos_integer() | undefined.
line({{line,L},_}) -> L;
line(_) -> undefined.
-endif.

-ifdef(column).
-spec column(index() | term()) -> pos_integer() | undefined.
column({_,{column,C}}) -> C;
column(_) -> undefined.
-endif.

-spec p_advance_index(input() | unicode:charlist() | pos_integer(), index()) -> index().
p_advance_index(MatchedInput, Index) when is_list(MatchedInput) orelse is_binary(MatchedInput)-> % strings
  lists:foldl(fun p_advance_index/2, Index, unicode:characters_to_list(MatchedInput));
p_advance_index(MatchedInput, Index) when is_integer(MatchedInput) -> % single characters
  {{line, Line}, {column, Col}} = Index,
  case MatchedInput of
    $\n -> {{line, Line+1}, {column, 1}};
    _ -> {{line, Line}, {column, Col+1}}
  end.
