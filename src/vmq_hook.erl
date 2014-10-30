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

-module(vmq_hook).

-export([start/1, stop/0]).
-export([declare/3]).
-export([add/2, delete/4]).
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
    Declarations =
    lists:foldl(
      fun(M, Acc) ->
              [[begin declare(Name, Type, Arity), {M, Name, Arity} end
               || {hook, [{Name, Type, Arity}]}
                  <- M:module_info(attributes)] | Acc]
      end, [], Modules),
    Registrations =
    lists:foldl(
      fun(M, Acc) ->
              [[begin add(Name, MFA), {M, Name, MFA} end
               || {register_hook, [{Name, MFA}]}
                  <- M:module_info(attributes)] | Acc]
      end, [], Modules),
    case {lists:flatten(Declarations), lists:flatten(Registrations)} of
        {[], []} ->
            ok;
        {FDeclarations, FRegistrations} ->
            error_logger:info_msg(
              "App ~p registered ~p hook-points, and registered ~p hooks~n",
              [App, FDeclarations, FRegistrations])
    end.


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

-spec add(name(), mfa()) -> ok | no_return().

add(Name, {Module, Function, Arity}) ->
    try
        ListOfTuple = apply(Module, module_info, [exports]),
        case lists:member({Function, Arity}, ListOfTuple) of
            true ->
                Hook = {Module, Function, Arity},
                case ets:lookup(?TABLE, Name) of
                    [] ->
                        error({missing_declare, Name});
                    [{Name, Type, Arity, ListOfHook}] ->
                        verify_add(Name, Type, Arity,
                                   ListOfHook, {length(ListOfHook), Hook});
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

-spec verify_add(_ , _, _, [any()], {non_neg_integer(),
                                     {atom() | tuple(), _, _}}) ->
                        'ok' | {'error', 'duplicate_mfa' |
                                'duplicate_priority'}.
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

-spec only0(maybe_improper_list(), _) -> any().
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

-spec every0(_, _, [any()]) -> any().
every0(Value, Args, ListOfHook) ->
    F = fun({_Priority, {Module, Function, Arity}}, Acc) ->
                try_apply_exit(Module, Function, [Acc|Args], Arity)
        end,
    lists:foldl(F, Value, ListOfHook).

-spec try_apply_exit(atom() | tuple(), atom(), [any()], _) -> any().
try_apply_exit(Module, Function, Args, Arity) ->
    try
        apply(Module, Function, Args)
    catch
        _Class:Reason ->
            error({invalid_apply, {Module, Function, Arity},
                   Reason})
    end.

-spec try_apply(atom() | tuple(), atom(), [any()], _) -> any().
try_apply(Module, Function, Args, _Arity) ->
    try
        apply(Module, Function, Args)
    catch
        _Class:_Reason ->
            next
    end.
