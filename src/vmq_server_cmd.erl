-module(vmq_server_cmd).
-export([node_start/0,
         node_stop/0,
         node_status/0,
         node_reset/0,
         node_reset/1,
         node_change_type/1,
         node_join/1,
         node_join/2,
         node_remove/1,
         node_remove_offline/1,
         node_upgrade/0,
         node_upgrade/1,
         list_sessions/0,
         list_sessions/1,
         set_config/2,
         listener_start/1,
         listener_start/2,
         listener_stop/2,
         listener_stop/3
        ]).

node_start() ->
    vmq_server_cli:command(["vmq-admin", "node", "start"], false).

node_stop() ->
    vmq_server_cli:command(["vmq-admin", "node", "stop"], false).

node_status() ->
    vmq_server_cli:command(["vmq-admin", "node", "status"], false).

node_reset() ->
    node_reset(false).
node_reset(false) ->
    vmq_server_cli:command(["vmq-admin", "node", "reset"], false);
node_reset(true) ->
    vmq_server_cli:command(["vmq-admin", "node", "reset", "--forcefully"], false).

node_change_type(disc) ->
    vmq_server_cli:command(["vmq-admin", "node", "chtype", "-t", "disc"], false);
node_change_type(ram) ->
    vmq_server_cli:command(["vmq-admin", "node", "chtype", "-t", "ram"], false).

node_join(DiscoveryNode) ->
    node_join(DiscoveryNode, disc).
node_join(DiscoveryNode, disc) ->
    vmq_server_cli:command(["vmq-admin", "node", "join", "discovery-node=" ++
             atom_to_list(DiscoveryNode), "-t", "disc"], false);
node_join(DiscoveryNode, ram) ->
    vmq_server_cli:command(["vmq-admin", "node", "join", "discovery-node=" ++
             atom_to_list(DiscoveryNode), "-t", "ram"], false).

node_remove(Node) ->
    vmq_server_cli:command(["vmq-admin", "node", "remove", "node=" ++
             atom_to_list(Node)], false).
node_remove_offline(Node) ->
    vmq_server_cli:command(["vmq-admin", "node", "remove", "node=" ++
             atom_to_list(Node), "offline"], false).

node_upgrade() ->
    vmq_server_cli:command(["vmq-admin", "node", "upgrade", "--upgrade-now"], false).
node_upgrade(InstructionFile) ->
    vmq_server_cli:command(["vmq-admin", "node", "upgrade", "--upgrade-now", "--instruction-file=" ++ InstructionFile], false).

list_sessions() ->
    list_sessions(vmq_session:info_items()).
list_sessions(InfoItems) ->
    vmq_server_cli:command(["vmq-admin", "session", "list"
             | ["--" ++ atom_to_list(I) || I <- InfoItems]], false).

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

listener_start(Port) ->
    listener_start(Port, []).

listener_start(Port, Opts) when is_integer(Port) and is_list(Opts) ->
    vmq_server_cli:command(["vmq-admin", "listener", "start", "port=" ++
                            integer_to_list(Port) | convert_listener_options(Opts, [])], false).

listener_stop(Port, Address) ->
    listener_stop(Port, Address, false).
listener_stop(Port, Address, false) when is_integer(Port) and is_list(Address) ->
    vmq_server_cli:command(["vmq-admin", "listener", "stop",
                            "--port", integer_to_list(Port),
                            "--address", Address], false);
listener_stop(Port, Address, false) when is_integer(Port) and is_list(Address) ->
    vmq_server_cli:command(["vmq-admin", "listener", "stop",
                            "--port", integer_to_list(Port),
                            "--address", Address,
                            "--kill_sessions"], false).



convert_listener_options([{K, true}|Rest], Acc) ->
    %% e.g. "--websocket"
    convert_listener_options(Rest, ["--" ++ atom_to_list(K) | Acc]);
convert_listener_options([{_K, false}|Rest], Acc) ->
    %% setting Val == false is the same as not setting the prop at all
    convert_listener_options(Rest, Acc);
convert_listener_options([{K, V}|Rest], Acc) when is_atom(V) ->
    convert_listener_options(Rest, ["--" ++ atom_to_list(K), atom_to_list(V) | Acc]);
convert_listener_options([{K, V}|Rest], Acc) when is_integer(V) ->
    convert_listener_options(Rest, ["--" ++ atom_to_list(K), integer_to_list(V) | Acc]);
convert_listener_options([{K, V}|Rest], Acc) when is_list(V) ->
    convert_listener_options(Rest, ["--" ++ atom_to_list(K), V | Acc]);
convert_listener_options([], Acc) -> Acc.



