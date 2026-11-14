-- Run once after 0001_accounts_follow.sql against the selected NoteX database.
CREATE TABLE posts (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    author_id BIGINT NOT NULL,
    content VARCHAR(500) CHARACTER SET utf8mb4 NOT NULL,
    created_on BIGINT NOT NULL,
    deleted_on BIGINT NULL,
    KEY idx_author_active_created (author_id, deleted_on, created_on DESC, id DESC),
    KEY idx_active_created (deleted_on, created_on DESC, id DESC),
    CONSTRAINT fk_post_author FOREIGN KEY (author_id) REFERENCES users (id)
) ENGINE=InnoDB;
