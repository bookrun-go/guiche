package guiche

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type CommitState interface {
	// 小于启动时offset
	LessThanStart(ctx context.Context, topic string, partition int32, offset kafka.Offset) (bool, error)
}
