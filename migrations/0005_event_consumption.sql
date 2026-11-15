-- Run once after 0004_outbox_events.sql.
CREATE TABLE consumption_jobs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    consumer VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_key VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id BIGINT NULL,
    message_id VARBINARY(255) NOT NULL,
    body LONGBLOB NOT NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    attempts BIGINT UNSIGNED NOT NULL DEFAULT 0,
    cycle_attempts INT UNSIGNED NOT NULL DEFAULT 0,
    next_attempt_on BIGINT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_on BIGINT NOT NULL,
    completed_on BIGINT NULL,
    failed_on BIGINT NULL,
    UNIQUE KEY uk_consumption_identity (consumer, event_key),
    KEY idx_consumption_pending (consumer, status, next_attempt_on, id)
) ENGINE=InnoDB;

CREATE TABLE event_replays (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    scope VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    record_id BIGINT NOT NULL,
    reason VARCHAR(1024) NOT NULL,
    previous_error TEXT NOT NULL,
    created_on BIGINT NOT NULL,
    KEY idx_replay_record (scope, record_id, id)
) ENGINE=InnoDB;

-- Only the explicit dummy command/test handler writes this verification table.
CREATE TABLE dummy_effects (
    consumer VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id BIGINT NOT NULL,
    event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload JSON NOT NULL,
    created_on BIGINT NOT NULL,
    PRIMARY KEY (consumer, event_id)
) ENGINE=InnoDB;
