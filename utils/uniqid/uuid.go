package uniqid

import (
	"github.com/go-basic/uuid"
)

func GenUuid() string {
	return uuid.New()
}