%% Test helpers
-define(ENDPOINT, "http://localhost:34567").
-define(PEER_BIN, <<"127.0.0.1">>).
-define(PEERPORT, 12345).
-define(PEER, {{127,0,0,1}, ?PEERPORT}).
-define(IGNORED_SUBSCRIBER_ID, <<"ignored-subscriber-id">>).
-define(ALLOWED_SUBSCRIBER_ID, <<"allowed-subscriber-id">>).
-define(NOT_ALLOWED_SUBSCRIBER_ID, <<"not-allowed-subscriber-id">>).
-define(SERVER_ERR_SUBSCIBER_ID, <<"internal-server-error">>).
-define(MOUNTPOINT, "mountpoint").
-define(MOUNTPOINT_BIN, <<"mountpoint">>).
-define(CHANGED_SUBSCRIBER_ID, <<"changed-subscriber-id">>).
-define(USERNAME, <<"test-user">>).
-define(PASSWORD, <<"test-password">>).
-define(TOPIC, <<"test/topic">>).
-define(PAYLOAD, <<"hello world">>).

