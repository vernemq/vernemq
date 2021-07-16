-module(vmq_jwt_auth_SUITE).

%% API
-export([
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2,
  all/0
]).

-export([
  auth_on_register_test/1,
  auth_on_register_username_contains_colon_test/1,
  auth_on_register_rid_absent_test/1,
  auth_on_register_rid_different_test/1,
  auth_on_register_unparsable_token_test/1
]).

all() ->
  [
    auth_on_register_test,
    auth_on_register_username_contains_colon_test,
    auth_on_register_rid_absent_test,
    auth_on_register_rid_different_test,
    auth_on_register_unparsable_token_test
  ].

init_per_suite(_Config) ->
  cover:start(),
  _Config.

end_per_suite(_Config) ->
  _Config.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_, Config) ->
  Config.

auth_on_register_test(_) ->
  ok = application:set_env(vmq_jwt_auth, secret_key, "test-key"),

  %When username contains no colons
  Password = jwerl:sign([{rid, <<"username">>}], hs256, <<"test-key">>),
  ok = vmq_jwt_auth:auth_on_register({"",""}, {"",""}, <<"username">>, Password, false),
  application:unset_env(vmq_jwt_auth, secret_key).

auth_on_register_rid_absent_test(_) ->
  ok = application:set_env(vmq_jwt_auth, secret_key, "test-key"),

  %When rid is not present in claims
  Password = jwerl:sign([{norid, <<"username">>}], hs256, <<"test-key">>),
  error = vmq_jwt_auth:auth_on_register({"",""}, {"",""}, "username", Password, false),
  application:unset_env(vmq_jwt_auth, secret_key).

auth_on_register_rid_different_test(_) ->
  ok = application:set_env(vmq_jwt_auth, secret_key, "test-key"),

  %When rid in claims is different from username
  Password = jwerl:sign([{rid, <<"different_username">>}], hs256, <<"test-key">>),
  error = vmq_jwt_auth:auth_on_register({"",""}, {"",""}, <<"username">>, Password, false),
  application:unset_env(vmq_jwt_auth, secret_key).

auth_on_register_unparsable_token_test(_) ->
  ok = application:set_env(vmq_jwt_auth, secret_key, "test-key"),

  %When password is not jwt from username
  {error, invalid_signature} = vmq_jwt_auth:auth_on_register({"",""}, {"",""}, <<"username">>, <<"Password">>, false),
  application:unset_env(vmq_jwt_auth, secret_key).

auth_on_register_username_contains_colon_test(_) ->
  ok = application:set_env(vmq_jwt_auth, secret_key, "test-key"),

  %When username contains colons
  Password = jwerl:sign([{rid, <<"username">>}], hs256, <<"test-key">>),
  ok = vmq_jwt_auth:auth_on_register({"",""}, {"",""}, <<"username:123:456:789">>, Password, false),
  application:unset_env(vmq_jwt_auth, secret_key).
