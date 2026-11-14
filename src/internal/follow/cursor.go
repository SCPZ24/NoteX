package follow

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

var ErrInvalidCursor = errors.New("invalid cursor")

// Owner and Direction bind a cursor to the list that produced it.
type cursor struct {
	Owner     int64  `json:"owner"`
	Direction string `json:"direction"`
	CreatedOn int64  `json:"created_on"`
	ID        int64  `json:"id"`
}

func decodeCursor(value string, owner int64, direction string) (cursor, error) {
	var result cursor
	if len(value) > 512 {
		return result, ErrInvalidCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return result, ErrInvalidCursor
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return result, ErrInvalidCursor
	}
	if result.Owner != owner || result.Direction != direction || result.CreatedOn <= 0 || result.ID <= 0 {
		return result, ErrInvalidCursor
	}
	return result, nil
}

func encodeCursor(value cursor) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}
