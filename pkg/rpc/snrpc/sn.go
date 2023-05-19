package snrpc

import "github.com/google/uuid"

func GenSN() string {
	return uuid.New().String()
}
