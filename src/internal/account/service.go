package account

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"regexp"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrInvalid       = errors.New("invalid username or password")
	ErrUsernameTaken = errors.New("username already exists")
	ErrUnauthorized  = errors.New("invalid credentials or session")
	ErrNotFound      = errors.New("user not found")
	usernamePattern  = regexp.MustCompile(`^[A-Za-z0-9_]{3,32}$`)
)

type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	CreatedOn int64  `json:"created_on"`
}

type LoginResult struct {
	User      User   `json:"user"`
	Token     string `json:"token"`
	ExpiresOn int64  `json:"expires_on"`
}

type Service struct{ db *sql.DB }

func New(db *sql.DB) *Service { return &Service{db: db} }

func (s *Service) Register(ctx context.Context, username, password string) (User, error) {
	if !usernamePattern.MatchString(username) || len(password) < 8 || len(password) > 72 {
		return User{}, ErrInvalid
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return User{}, err
	}
	user := User{Username: username, CreatedOn: time.Now().Unix()}
	result, err := s.db.ExecContext(ctx,
		"INSERT INTO users (username, password_hash, created_on) VALUES (?, ?, ?)",
		username, string(hash), user.CreatedOn)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return User{}, ErrUsernameTaken
		}
		return User{}, err
	}
	user.ID, err = result.LastInsertId()
	return user, err
}

func (s *Service) Login(ctx context.Context, username, password string) (LoginResult, error) {
	if !usernamePattern.MatchString(username) || len(password) < 8 || len(password) > 72 {
		return LoginResult{}, ErrUnauthorized
	}
	var user User
	var hash string
	err := s.db.QueryRowContext(ctx,
		"SELECT id, username, created_on, password_hash FROM users WHERE username = ?", username).
		Scan(&user.ID, &user.Username, &user.CreatedOn, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return LoginResult{}, ErrUnauthorized
	}
	if err != nil {
		return LoginResult{}, err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return LoginResult{}, ErrUnauthorized
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return LoginResult{}, err
	}
	token := hex.EncodeToString(random[:])
	tokenHash := sha256.Sum256([]byte(token))
	expires := time.Now().Add(24 * time.Hour).Unix()
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO sessions (token_hash, user_id, expires_on) VALUES (?, ?, ?)",
		tokenHash[:], user.ID, expires)
	if err != nil {
		return LoginResult{}, err
	}
	return LoginResult{User: user, Token: token, ExpiresOn: expires}, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (User, error) {
	if len(token) != 64 {
		return User{}, ErrUnauthorized
	}
	if _, err := hex.DecodeString(token); err != nil {
		return User{}, ErrUnauthorized
	}
	hash := sha256.Sum256([]byte(token))
	var user User
	err := s.db.QueryRowContext(ctx, `SELECT u.id, u.username, u.created_on
		FROM sessions s JOIN users u ON u.id = s.user_id
		WHERE s.token_hash = ? AND s.expires_on > ?`, hash[:], time.Now().Unix()).
		Scan(&user.ID, &user.Username, &user.CreatedOn)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUnauthorized
	}
	return user, err
}

func (s *Service) Logout(ctx context.Context, token string) error {
	hash := sha256.Sum256([]byte(token))
	_, err := s.db.ExecContext(ctx, "DELETE FROM sessions WHERE token_hash = ?", hash[:])
	return err
}

func (s *Service) FindByUsername(ctx context.Context, username string) (User, error) {
	if !usernamePattern.MatchString(username) {
		return User{}, ErrInvalid
	}
	var user User
	err := s.db.QueryRowContext(ctx,
		"SELECT id, username, created_on FROM users WHERE username = ?", username).
		Scan(&user.ID, &user.Username, &user.CreatedOn)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return user, err
}
