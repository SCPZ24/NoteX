-- Bind durable tasks to one topology even if the etcd namespace is recreated.
CREATE TABLE feed_topologies (
    queue_prefix VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    shard_count INT NOT NULL,
    policy_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL
) ENGINE=InnoDB;

-- A confirmed retry is owned by the durable MQ queue until receipt. A publish
-- whose outcome is uncertain becomes eligible again after its short claim.
ALTER TABLE consumption_jobs
    ADD COLUMN retry_publish_after BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN retry_publish_token VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '';
