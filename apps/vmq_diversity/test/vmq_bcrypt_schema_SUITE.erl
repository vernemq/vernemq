-module(vmq_bcrypt_schema_SUITE).

-compile([nowarn_export_all, export_all]).
-compile(nowarn_deprecated_function).

-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
  application:ensure_all_started(cuttlefish),
  Config.

end_per_suite(_Config) ->
  ok.

all() ->
  [bcrypt_port_pool_auto_test,
   bcrypt_nif_pool_auto_test,
   bcrypt_port_pool_test,
   bcrypt_nif_pool_test,
   bcrypt_invalid_log_rounds_test].

bcrypt_port_pool_auto_test(_Config) ->
  AutoSize = expect([{["vmq_bcrypt", "pool_size"], "auto"}], [bcrypt, pool_size]),
  ?assert(is_integer(AutoSize) and (AutoSize > 0)).

bcrypt_nif_pool_auto_test(_Config) ->
  AutoSize = expect([{["vmq_bcrypt", "nif_pool_size"], "auto"}], [bcrypt, nif_pool_size]),
  ?assert(is_integer(AutoSize) and (AutoSize > 0)).

bcrypt_port_pool_test(_Config) ->
  11 = expect([{["vmq_bcrypt", "pool_size"], "11"}], [bcrypt, pool_size]).

bcrypt_nif_pool_test(_Config) ->
  11 = expect([{["vmq_bcrypt", "nif_pool_size"], "11"}], [bcrypt, nif_pool_size]).

bcrypt_invalid_log_rounds_test(_Config) ->
  Schema = get_schema(),
  Conf = [{["vmq_bcrypt", "default_log_rounds"], "50"}],
  ?assertMatch({error, validation, _}, cuttlefish_generator:map(Schema, Conf)).

-define(stacktrace,
        try
          throw(foo)
        catch
          _:foo:Stacktrace ->
            Stacktrace
        end).

expect(Conf, Setting) ->
  Schema = get_schema(),
  case cuttlefish_generator:map(Schema, Conf) of
    {error, _, _} = E ->
      StackTrace = ?stacktrace,
      throw({E, StackTrace});
    Gen ->
      deep_find(Gen, Setting)
  end.

deep_find(Value, []) ->
  Value;
deep_find(Conf, [Prop | T]) ->
  case lists:keyfind(Prop, 1, Conf) of
    false ->
      StackTrace = ?stacktrace,
      throw({could_not_find, Prop, in, Conf, StackTrace});
    {Prop, Value} ->
      deep_find(Value, T)
  end.

get_schema() ->
  cuttlefish_schema:files([code:priv_dir(vmq_diversity) ++ "/vmq_bcrypt.schema"]).
