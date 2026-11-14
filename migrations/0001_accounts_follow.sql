-- Run once against the selected NoteX database, before starting the API.
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    password_hash VARCHAR(60) CHARACTER SET ascii NOT NULL,
    created_on BIGINT NOT NULL,
    UNIQUE KEY uk_username (username)
) ENGINE=InnoDB;

CREATE TABLE sessions (
    token_hash BINARY(32) NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    expires_on BIGINT NOT NULL,
    KEY idx_session_user (user_id),
    KEY idx_session_expiry (expires_on),
    CONSTRAINT fk_session_user FOREIGN KEY (user_id) REFERENCES users (id)
) ENGINE=InnoDB;

CREATE TABLE following (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    follow_id BIGINT NOT NULL,
    created_on BIGINT NOT NULL,
    UNIQUE KEY uk_user_follow (user_id, follow_id),
    KEY idx_follow_created (follow_id, created_on DESC, id DESC),
    KEY idx_user_created (user_id, created_on DESC, id DESC),
    CONSTRAINT fk_follow_actor FOREIGN KEY (user_id) REFERENCES users (id),
    CONSTRAINT fk_follow_target FOREIGN KEY (follow_id) REFERENCES users (id)
) ENGINE=InnoDB;
