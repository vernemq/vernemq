%% Test helpers
-define(PEER_BIN, <<"127.0.0.1">>).
-define(PEERPORT, 12345).
-define(PEER, {{127,0,0,1}, ?PEERPORT}).
-define(ALLOWED_CLIENT_ID, <<"allowed-subscriber-id">>).
-define(MOUNTPOINT, "mountpoint").
-define(MOUNTPOINT_BIN, <<"mountpoint">>).
-define(TOPIC, <<"test/topic">>).
-define(PAYLOAD, <<"hello world">>).
-define(REASON, 'REASON_NORMAL_DISCONNECT').

