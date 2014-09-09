-module(emqttd_hook).

-export([start/1, stop/0]).
-export([declare/3]).
-export([add/3, delete/4]).
-export([all/2, only/2, every/3]).

-define(TABLE, ?MODULE).
-type priority() :: non_neg_integer().
-type args() :: [any()].
-type value() :: any().
-type name() :: atom().
-type type() :: all | only | every.


-spec start(atom()) -> ok .
start(App) ->
    case lists:member(?TABLE, ets:all()) of
        true ->
            ok;
        false ->
            _Tid = ets:new(?TABLE,
                           [set, public, named_table, {read_concurrency, true}])
    end,
    {ok, Modules} = application:get_key(App, modules),
    lists:foreach(
      fun(M) ->
              [declare(Name, Type, Arity)
               || {hook, [{Name, Type, Arity}]}
                  <- M:module_info(attributes)]
      end, Modules),
    lists:foreach(
      fun(M) ->
              [add(Name, ets:info(?TABLE, size), MFA)
               || {register_hook, [{Name, MFA}]}
                  <- M:module_info(attributes)]
      end, Modules).

-spec stop() -> ok.
stop() ->
    true = ets:delete(?TABLE),
    ok.

-spec declare(atom(), type(), arity()) -> ok.
declare(Name, Type, Arity) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            true = ets:insert(?TABLE, {Name, Type, Arity, []}),
            ok;
        _ ->
            {error, duplicate_name}
    end.

-spec add(name(), priority(), mfa()) -> ok | no_return().
add(Name, Priority, {Module, Function, Arity}) ->
    try
        ListOfTuple = apply(Module, module_info, [exports]),
        case lists:member({Function, Arity}, ListOfTuple) of
            true ->
                Hook = {Priority, {Module, Function, Arity}},
                case ets:lookup(?TABLE, Name) of
                    [] ->
                        error({missing_declare, Name});
                    [{Name, Type, Arity, ListOfHook}] ->
                        verify_add(Name, Type, Arity, ListOfHook, Hook);
                    [{_Name, Type, _Arity, _ListOfHook}] ->
                        error({invalid_arity, Name, Type, Hook})
                end;
            false ->
                error({undef_function, Module, Function, Arity})
        end
    catch
        error:undef ->
            error({undef_module, Module})
    end.

verify_add(Name, Type, Arity, ListOfHook, {Priority, MFA} = Hook) ->
    case lists:keymember(Priority, 1, ListOfHook) of
        false ->
            case lists:keymember(MFA, 2, ListOfHook) of
                false ->
                    NewListOfHook = lists:merge(ListOfHook, [Hook]),
                    true = ets:insert(?TABLE, {Name, Type, Arity,
                                               NewListOfHook}),
                    ok;
                true ->
                    {error, duplicate_mfa}
            end;
        true ->
            {error, duplicate_priority}
    end.

-spec delete(name(), type(), priority(), mfa()) -> ok.
delete(Name, Type, Priority, {Module, Function, Arity}) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            error({missing_declare, Name});
        [{Name, Type, Arity, ListOfHook}] ->
            Hook = {Priority, {Module, Function, Arity}},
            case lists:delete(Hook, ListOfHook) of
                [] ->
                    true = ets:delete(?TABLE, Name),
                    ok;
                NewListOfHook ->
                    true = ets:insert(?TABLE, {Name, Type, Arity,
                                               NewListOfHook}),
                    ok
            end
    end.

-spec all(atom(), args()) -> [any()].
all(Name, Args) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            error({missing_declare, Name});
        [{Name, all, _Arity, ListOfHook}] ->
            F = fun({_, {Module, Function, Arity}}) ->
                        try_apply(Module, Function, Args, Arity)
                end,
            lists:map(F, ListOfHook)
    end.

-spec only(atom(), args()) -> not_found | any().
only(Name, Args) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            error({missing_declare, Name});
        [{Name, only, _Arity, ListOfHook}] ->
            only0(ListOfHook, Args)
    end.

only0([], _Args) ->
    not_found;
only0([{_Priority, {Module, Function, Arity}}|Rest], Args) ->
    case try_apply(Module, Function, Args, Arity) of
        next ->
            only0(Rest, Args);
        Value ->
            Value
    end.

-spec every(atom(), value(), args()) -> any().
every(Name, Value, Args) ->
    case ets:lookup(?TABLE, Name) of
        [] ->
            error({missing_declare, Name});
        [{_Name, every, _Arity, ListOfHook}] ->
            every0(Value, Args, ListOfHook)
    end.

every0(Value, Args, ListOfHook) ->
    F = fun({_Priority, {Module, Function, Arity}}, Acc) ->
                try_apply(Module, Function, [Acc|Args], Arity)
        end,
    lists:foldl(F, Value, ListOfHook).

try_apply(Module, Function, Args, Arity) ->
    try
        apply(Module, Function, Args)
    catch
        _Class:Reason ->
            error({invalid_apply, {Module, Function, Arity},
                   Reason})
    end.
