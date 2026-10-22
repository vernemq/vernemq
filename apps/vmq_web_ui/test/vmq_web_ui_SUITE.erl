-module(vmq_web_ui_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_testcase(_Case, Config) ->
    catch ets:delete(webuitoken),
    Config.

end_per_testcase(_Case, Config) ->
    catch ets:delete(webuitoken),
    Config.

all() ->
    [routes_multiple_listeners].

routes_multiple_listeners(_) ->
    Routes = vmq_web_ui:routes(),
    ?assertEqual(Routes, vmq_web_ui:routes()),
    ?assertEqual(Routes, vmq_web_ui:routes()),
    ?assertNotEqual(undefined, ets:whereis(webuitoken)).
