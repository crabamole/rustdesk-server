CREATE TABLE IF NOT EXISTS peer (
    guid bytea PRIMARY KEY NOT NULL,
    id varchar(100) NOT NULL,
    uuid bytea NOT NULL,
    pk bytea NOT NULL,
    created_at text NOT NULL DEFAULT current_timestamp,
    "user" bytea,
    status smallint NOT NULL DEFAULT 1,
    note varchar(300),
    region text,
    strategy bytea,
    info text NOT NULL DEFAULT '{}',
    last_online text NOT NULL DEFAULT '2011-11-16 11:55:19'
);
CREATE UNIQUE INDEX IF NOT EXISTS index_peer_id ON peer (id);
CREATE INDEX IF NOT EXISTS index_peer_user ON peer ("user");
CREATE INDEX IF NOT EXISTS index_peer_created_at ON peer (created_at);
CREATE INDEX IF NOT EXISTS index_peer_status ON peer (status)
