-module(vmq_config_reloader).

-export([reload_all/1,
         reload_app_conf/1]).
-define(CONF_PATH, "data/generated.configs").

reload_all([]) ->
    %% called from vmq-admin script
    reload_app_conf(vmq_acl),
    reload_app_conf(vmq_passwd),
    %reload_app_conf(vmq_lua),
    reload_app_conf(vmq_exo),
    reload_app_conf(vmq_bridge),
    reload_app_conf(vmq_server).

reload_app_conf(Application) ->
    case config_diff(Application) of
        {ok, {New, Changed, Deleted}} ->
            apply_config_diff(Application, New, Changed, Deleted);
        E -> E
    end.

config_diff(Application) ->
    case file:list_dir(?CONF_PATH) of
        {ok, Filenames}  ->
            case sorted_sys_configs(Filenames, []) of
                [{_, SysConfFile}|_] ->
                    {ok, [SysConf]} = file:consult(filename:join(?CONF_PATH, SysConfFile)),
                    NewConf = proplists:get_value(Application, SysConf, []),
                    {New, Changed} = config_diffp(Application, NewConf, [], []),
                    case config_diffm(Application, NewConf) of
                        {Deleted, []} ->
                            {ok, {New, Changed, Deleted}};
                        {Deleted, Warn} ->
                            error_logger:warning_msg("Settings present in original ~p.app file but not in sys.config, it is safer to keep the configs ~p", [Application, Warn]),
                            {ok, {New, Changed, Deleted}}
                    end;
                _ ->
                    {error, no_sys_conf}
            end;
        E ->
            E
    end.

apply_config_diff(Application, New, Changed, Deleted) ->
    [application:set_env(Application, Par, Val) || {Par, Val} <- New],
    [application:set_env(Application, Par, Val) || {Par, {_, Val}} <- Changed],
    [application:unset_env(Application, Par) || {Par, _} <- Deleted],
    {ok, Modules} = application:get_key(Application, modules),
    lists:foreach(fun(Mod) ->
                        Exports = Mod:module_info(exports),
                        case lists:member({change_config_now, 3}, Exports) of
                            true ->
                                catch apply(Mod, change_config_now, [New, Changed, Deleted]);
                            false ->
                                ok
                        end
                end, Modules),
    error_logger:info_msg("reload config for application ~p~nNEW: ~p~nCHANGED: ~p~nDELETED: ~p~n",
                          [Application, New, Changed, Deleted]).


config_diffp(Application, [{Par, Val} = Prop | Rest], New, Changed) ->
    case application:get_env(Application, Par) of
        undefined ->
            config_diffp(Application, Rest, [Prop|New], Changed);
        {ok, Val} ->
            %% unchanged property
            config_diffp(Application, Rest, New, Changed);
        {ok, OldVal} ->
            config_diffp(Application, Rest, New, [{Par, {OldVal, Val}}|Changed])
    end;
config_diffp(_, [], New, Changed) -> {New, Changed}.

config_diffm(Application, NewConf) ->
    CurrentConf = application:get_all_env(Application),
    {ok, Vsn} = application:get_key(Application, vsn),
    StrApp = atom_to_list(Application),
    DotAppFile = filename:join(["lib", StrApp ++ "-"++ Vsn, "ebin", StrApp ++ ".app"]),
    {ok, [{application, Application, KVs}]} = file:consult(DotAppFile),
    OrigEnv = proplists:get_value(env, KVs, []),
    config_diffm(OrigEnv, NewConf, CurrentConf, [], []).


config_diffm(OrigEnv, NewConf, [{Par, _} = Prop | Rest], Deleted, Warn) ->
    case proplists:get_value(Par, NewConf) of
        undefined ->
            case lists:keymember(Par, 1, OrigEnv) of
                true ->
                    %% Setting present in Original Config app.src
                    %% but not exposed in sys.config. It is safer
                    %% to keep this config value
                    config_diffm(OrigEnv, NewConf, Rest, Deleted, [Prop|Warn]);
                false ->
                    config_diffm(OrigEnv, NewConf, Rest, [Prop|Deleted], Warn)
            end;
        _ ->
            config_diffm(OrigEnv, NewConf, Rest, Deleted, Warn)
    end;
config_diffm(_, _, [], Deleted, Warn) -> {Deleted, Warn}.

sorted_sys_configs([], SysConfigs) ->
    lists:reverse(lists:keysort(1, SysConfigs));
sorted_sys_configs([F|Rest], SysConfigs) ->
    case string:tokens(F, ".") of
        ["app", Y, M, D, H, Mm, S, _] ->
            sorted_sys_configs(Rest, [{{s(Y), s(M), s(D), s(H), s(Mm), s(S)}, F}|SysConfigs]);
        _ ->
            sorted_sys_configs(Rest, SysConfigs)
    end.

s(L) when is_list(L) ->
    list_to_integer(L).
