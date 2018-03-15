-module(test_compat_mod).

-compile(export_all).
-compile(nowarn_export_all).

sample_hook_v1_to_v0(sample_hook_name_v0, Mod, Fun, [A1, A2, A3]) ->
    %% transform arguments and return values as needed. For now we
    %% just keep it simple and remove one argument and add it back to
    %% the return value of the v0 hook.
    {ok, {A1, A2}} = apply(Mod, Fun, [A1, A2]),
    {ok, {A1, A2, A3}}.

sample_hook_v0(A1, A2) ->
    {ok, {A1,A2}}.
