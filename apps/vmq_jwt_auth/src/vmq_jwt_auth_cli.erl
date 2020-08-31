-module(vmq_jwt_auth_cli).

-export([register/0]).

register() ->
  ConfigKeys =
    ["vmq_jwt_auth.secret_key"],
  ok = clique:register_config_whitelist(ConfigKeys).
