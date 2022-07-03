package guiche

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KMessage struct {
	ctx context.Context
	msg *kafka.Message
}

var uiniqeKey = struct{}{}

func CheckUnique(ctx context.Context) bool {
	res := ctx.Value(uiniqeKey)
	if res == nil {
		return true
	}

	check, ok := res.(bool)
	if !ok {
		return true
	}

	return check
}

func WithCheckUnique(ctx context.Context, check bool) context.Context {
	return context.WithValue(ctx, uiniqeKey, check)
}
