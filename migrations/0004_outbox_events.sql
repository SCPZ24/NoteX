-- Run once after 0003_post_requests.sql, before starting the updated API.
CREATE TABLE outbox_events (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload JSON NOT NULL,
    created_on BIGINT NOT NULL,
    published_on BIGINT NULL,
    attempts BIGINT UNSIGNED NOT NULL DEFAULT 0,
    next_attempt_on BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(1024) NULL,
    failed_on BIGINT NULL,
    KEY idx_outbox_pending (published_on, failed_on, next_attempt_on, id)
) ENGINE=InnoDB;
