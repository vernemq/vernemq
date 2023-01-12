-- migrations/1667211768-messages.sql
-- :up
-- Up migration
CREATE TABLE messages (
    sid bytea,
    msgref bytea,
    payload bytea,
    created_time TIMESTAMPTZ DEFAULT Now(),

    PRIMARY KEY(sid, msgref)
);

CREATE INDEX messages_sid_idx on messages(sid);

-- :down
-- Down migration
DROP TABLE messages;
