-module(vmq_app_utils).

-export([ensure_all_started/1,
         ensure_all_stopped/1]).

-export([start_all_require_vmq_server/1,
         stop_all_require_vmq_server/1]).

-export([mnesia_cluster_reset/1,
         mnesia_cluster_force_reset/1,
         mnesia_cluster_join/1,
         mnesia_cluster_status/1,
         mnesia_cluster_change_node_type/1,
         mnesia_cluster_forget_node/1,
         mnesia_cluster_update_node/1]).

-export([upgrade/1]).

-export([list_sessions/1,
         session_info/1]).

ensure_all_started(App) ->
    application:ensure_all_started(App).

ensure_all_stopped(App)  ->
    ensure_all_stopped([App], []).

ensure_all_stopped([kernel|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([stdlib|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([lager|Apps], Res)  ->
    ensure_all_stopped(Apps, Res);
ensure_all_stopped([App|Apps], Res)  ->
    {ok, Deps} = application:get_key(App, applications),
    application:stop(App),
    Stopped = ensure_all_stopped(lists:reverse(Deps), []),
    ensure_all_stopped(Apps -- Stopped, [[App|Stopped]|Res]);
ensure_all_stopped([], Res) -> Res.


stop_all_require_vmq_server([]) ->
    %% stops all applications that are useless without vmq_server
    ensure_all_stopped(vmq_bridge),
    ensure_all_stopped(vmq_acl),
    ensure_all_stopped(vmq_passwd),
    ensure_all_stopped(vmq_exo),
    ensure_all_stopped(vmq_server).

start_all_require_vmq_server([]) ->
    ensure_all_started(vmq_server),
    ensure_all_started(vmq_exo),
    ensure_all_started(vmq_passwd),
    ensure_all_started(vmq_acl),
    ensure_all_started(vmq_bridge).


mnesia_cluster_reset([]) ->
    mnesia_cluster_op(fun() ->
                              mnesia_cluster_utils:reset()
                      end).

mnesia_cluster_force_reset([]) ->
    mnesia_cluster_op(fun() ->
                              mnesia_cluster_utils:force_reset()
                      end).

mnesia_cluster_join([DiscoveryNode]) ->
    mnesia_cluster_join([DiscoveryNode, "--disc"]);
mnesia_cluster_join([DiscoveryNode, NodeType]) ->
    NNodeType =
    case NodeType of
        "--disc" -> disc;
        "--ram" -> ram
    end,
    mnesia_cluster_op(
      fun() ->
              mnesia_cluster_utils:join_cluster(list_to_atom(DiscoveryNode), NNodeType)
      end).

mnesia_cluster_status([]) ->
    Status = mnesia_cluster_utils:status(),
    io:format("~p~n", [Status]).

mnesia_cluster_change_node_type([NodeType]) ->
    mnesia_cluster_op(
      fun() ->
              case NodeType of
                  "disc" ->
                      mnesia_cluster_utils:change_cluster_node_type(disc);
                  "ram" ->
                      mnesia_cluster_utils:change_cluster_node_type(ram)
              end
      end).

mnesia_cluster_forget_node([Node]) ->
    mnesia_cluster_op(
      fun() ->
              mnesia_cluster_utils:forget_cluster_node(list_to_atom(Node), false)
      end);
mnesia_cluster_forget_node([Node, "--offline"]) ->
    mnesia_cluster_op(
      fun() ->
              mnesia_cluster_utils:forget_cluster_node(list_to_atom(Node), true)
      end).

mnesia_cluster_update_node([Node]) ->
    mnesia_cluster_op(
      fun() ->
              mnesia_cluster_utils:update_cluster_node(list_to_atom(Node))
      end).

mnesia_cluster_op(Fun) ->
    try
        case Fun() of
            ok -> ok;
            Other -> io:format("~p~n", [Other])
        end
    catch
       Error ->
            io:format("~p~n", [Error])
    end.

upgrade([]) ->
    Ret = vmq_updo:dry_run(),
    io:format("~p~n", [Ret]),
    io:format("run with --upgrade-now to perform the upgrade~n");
upgrade(["--upgrade-now"]) ->
    Ret = vmq_updo:run(),
    io:format("~p~n", [Ret]).

list_sessions([_|InfoItems]) ->
    Ret = vmq_session:list_sessions(InfoItems),
    io:format("~p~n", [Ret]).

session_info([_, ClientId|InfoItems]) ->
    Ret = vmq_session:get_info(ClientId, InfoItems),
    io:format("~p~n", [Ret]).
