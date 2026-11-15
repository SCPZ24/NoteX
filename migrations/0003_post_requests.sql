-- Run once after 0002_posts.sql. Keep request records for at least 24 hours.
-- This version retains them indefinitely, including after a post is deleted.
CREATE TABLE post_requests (
    user_id BIGINT NOT NULL,
    request_key VARBINARY(128) NOT NULL,
    content VARCHAR(500) CHARACTER SET utf8mb4 NOT NULL,
    post_id BIGINT NULL,
    response_json JSON NULL,
    created_on BIGINT NOT NULL,
    PRIMARY KEY (user_id, request_key),
    CONSTRAINT fk_post_request_user FOREIGN KEY (user_id) REFERENCES users (id)
) ENGINE=InnoDB;
