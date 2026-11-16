-- Immutable Feed task input and frozen page handoffs. Execution state lives in
-- consumption_jobs, at root/page/chunk granularity.
CREATE TABLE feed_tasks (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    root_event_id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL COMMENT '0 for the first plan of a root event',
    ordinal INT NOT NULL COMMENT 'Stable position within the frozen parent page',
    kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    body JSON NOT NULL,
    children JSON NULL COMMENT 'NULL before planning; frozen child IDs after planning, including []',
    created_on BIGINT NOT NULL,
    UNIQUE KEY uk_feed_child (root_event_id, parent_id, ordinal)
) ENGINE=InnoDB;
