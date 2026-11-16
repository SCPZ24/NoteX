-- Run once after 0003_post_requests.sql, before starting the updated API.
-- Event payload contract:
-- post.created / post.deleted: {"post_id": 9001}
-- follow.created / follow.deleted:
--   {"actor_id": 7, "target_user_id": 42, "relation_id": 501}
-- relation_id is following.id for the relationship created or deleted by the
-- business transaction. Capture it before deleting the relationship row.
-- Consumers read current authoritative data using these business IDs.
CREATE TABLE outbox_events (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique event identity',
    event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL
        COMMENT 'post.created, post.deleted, follow.created, follow.deleted',
    payload JSON NOT NULL COMMENT 'Business IDs for the event type; see payload contract above',
    created_on BIGINT NOT NULL COMMENT 'Event creation time, Unix seconds',
    published_on BIGINT NULL COMMENT 'Time all required MQ destinations confirmed publication, Unix seconds',
    attempts BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Number of publication attempts',
    next_attempt_on BIGINT NOT NULL DEFAULT 0 COMMENT 'Earliest publication retry time, Unix seconds; 0 means eligible immediately',
    last_error VARCHAR(1024) NULL COMMENT 'Most recent publication failure reason',
    failed_on BIGINT NULL COMMENT 'Time publication was marked failed for operator intervention, Unix seconds',
    KEY idx_outbox_pending (published_on, failed_on, next_attempt_on, id)
) ENGINE=InnoDB;
