package guiche

import (
	"github.com/bookrun-go/guiche/glog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewCoordinatorFactory(newHandle func() Handle, logger glog.Logger, maxRunner int, consumer *kafka.Consumer, commitState CommitState) *Coordinator {
	// todo  最大容量可配 ，合理的值应该是 最大runner数*runner最大消息数
	delKeyChan := make(chan *delKey, 100)

	newRunner := func() *Runner {
		return NewRunner(newHandle, logger, delKeyChan)
	}

	controller := NewRunnerController(newRunner, delKeyChan, maxRunner)

	return NewCoordinator(consumer, controller, commitState, logger)
}
