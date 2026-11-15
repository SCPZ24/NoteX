-- Run once after 0003_post_requests.sql, before starting the updated API.
CREATE TABLE outbox_events (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload JSON NOT NULL,
    created_on BIGINT NOT NULL
) ENGINE=InnoDB;
