-module(vmq_server_cmd).
-export([node_start/0,
         node_stop/0,
         node_status/0,
         node_join/1,
         node_leave/1,
         node_upgrade/0,
         node_upgrade/1,
         set_config/2,
         listener_start/2,
         listener_start/3,
         listener_stop/2,
         listener_stop/3,
         listener_delete/2,
         metrics/0
        ]).

node_start() ->
    vmq_server_cli:command(["vmq-admin", "node", "start"], false).

node_stop() ->
    vmq_server_cli:command(["vmq-admin", "node", "stop"], false).

node_status() ->
    vmq_server_cli:command(["vmq-admin", "cluster", "status"], false).

node_join(DiscoveryNode) ->
    vmq_server_cli:command(["vmq-admin", "cluster", "join", "discovery-node=" ++
             atom_to_list(DiscoveryNode)], false).

node_leave(Node) ->
    vmq_server_cli:command(["vmq-admin", "cluster", "leave", "node=" ++
             atom_to_list(Node), "--kill_sessions"], false).

node_upgrade() ->
    vmq_server_cli:command(["vmq-admin", "node", "upgrade", "--upgrade-now"], false).
node_upgrade(InstructionFile) ->
    vmq_server_cli:command(["vmq-admin", "node", "upgrade", "--upgrade-now", "--instruction-file=" ++ InstructionFile], false).

set_config(Key, true) ->
    set_config(Key, "on");
set_config(Key, false) ->
    set_config(Key, "off");
set_config(Key, Val) when is_atom(Val) ->
    set_config(Key, atom_to_list(Val));
set_config(Key, Val) when is_integer(Val) ->
    set_config(Key, integer_to_list(Val));
set_config(Key, Val) when is_list(Val) and is_atom(Key) ->
    vmq_server_cli:command(["vmq-admin", "set", atom_to_list(Key) ++ "=" ++ Val], false).

listener_start(Port, Opts) ->
    listener_start(Port, "127.0.0.1", Opts).

listener_start(Port, Addr, Opts) when is_integer(Port) and is_list(Addr) and is_list(Opts) ->
    vmq_server_cli:command(["vmq-admin", "listener", "start", "port=" ++
                            integer_to_list(Port), "address=" ++ Addr
                            | convert_listener_options(Opts, [])], false).

listener_stop(Port, Address) ->
    listener_stop(Port, Address, false).
listener_stop(Port, Address, false) when is_integer(Port) and is_list(Address) ->
    vmq_server_cli:command(["vmq-admin", "listener", "stop",
                            "port=" ++ integer_to_list(Port),
                            "address=" ++ Address], false);
listener_stop(Port, Address, true) when is_integer(Port) and is_list(Address) ->
    vmq_server_cli:command(["vmq-admin", "listener", "stop",
                            "port=" ++ integer_to_list(Port),
                            "address=" ++ Address,
                            "--kill_sessions"], false).

listener_delete(Port, Address) when is_integer(Port) and is_list(Address) ->
    vmq_server_cli:command(["vmq-admin", "listener", "delete",
                            "port=" ++ integer_to_list(Port),
                            "address=" ++ Address], false).

metrics() ->
    vmq_server_cli:command(["vmq-admin", "metrics", "show"], false).


convert_listener_options([{K, true}|Rest], Acc) ->
    %% e.g. "--websocket"
    convert_listener_options(Rest, ["--" ++ atom_to_list(K) | Acc]);
convert_listener_options([{K, V}|Rest], Acc) when is_atom(V) ->
    convert_listener_options(Rest, ["--" ++ atom_to_list(K), atom_to_list(V) | Acc]);
convert_listener_options([{K, V}|Rest], Acc) when is_integer(V) ->
    convert_listener_options(Rest, ["--" ++ atom_to_list(K), integer_to_list(V) | Acc]);
convert_listener_options([{K, V}|Rest], Acc) when is_list(V) ->
    convert_listener_options(Rest, ["--" ++ atom_to_list(K), V | Acc]);
convert_listener_options([], Acc) -> Acc.



