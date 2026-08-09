%% Test helpers
-define(HTTPS_ENDPOINT, "https://localhost:45678").
-define(ENDPOINT, "http://localhost:34567").
-define(PEER_BIN, <<"127.0.0.1">>).
-define(PEERPORT, 12345).
-define(PEER, {{127,0,0,1}, ?PEERPORT}).
-define(IGNORED_CLIENT_ID, <<"ignored-subscriber-id">>).
-define(ALLOWED_CLIENT_ID, <<"allowed-subscriber-id">>).
-define(LISTENER_INFO_CLIENT_ID, <<"listener-info">>).
-define(LISTENER_INFO_UNIX_CLIENT_ID, <<"listener-info-unix">>).
-define(BASE64_PAYLOAD_CLIENT_ID, <<"payload-is-base64-encoded">>).
-define(NO_PAYLOAD_CLIENT_ID, <<"no-payload">>).
-define(WITH_PROPERTIES, <<"with_properties">>).
-define(NOT_ALLOWED_CLIENT_ID, <<"not-allowed-subscriber-id">>).
-define(SERVER_ERR_SUBSCIBER_ID, <<"internal-server-error">>).
-define(MOUNTPOINT, "mountpoint").
-define(MOUNTPOINT_BIN, <<"mountpoint">>).
-define(CHANGED_CLIENT_ID, <<"changed-subscriber-id">>).
-define(CANCEL_CLIENT_ID, <<"cancel-auth-register">>).
-define(CANCEL_CLIENT_ID_M5, <<"cancel-auth-register-m5">>).
-define(TIMEOUT_CLIENT_ID, <<"timeout-auth-register">>).
-define(USERNAME, <<"test-user">>).
-define(CHANGED_USERNAME, <<"changed-user">>).
-define(PASSWORD, <<"test-password">>).
-define(TOPIC, <<"test/topic">>).
-define(PAYLOAD, <<"hello world">>).
-define(OPTS, #{
    client_cert => <<"client cert">>,
    listener_addr => {127, 0, 0, 1},
    listener_port => 1883,
    listener_type => mqtt
}).
-define(OPTS_UNIX_SOCKET, #{
    listener_addr => {local, "/tmp/vmq_test.sock"},
    listener_port => 0,
    listener_type => mqtt
}).
