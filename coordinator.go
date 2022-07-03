package guiche

import (
	"context"
	"log"
	"time"

	"github.com/bookrun-go/guiche/glog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	pollTimeout = time.Second
)

type Coordinator struct {
	consumer *kafka.Consumer
	// runController *RunController
	commitState CommitState
	logger      glog.Logger
}

func (coo *Coordinator) Start() {
	log.Print()
	for {
		evn := coo.consumer.Poll(int(pollTimeout.Milliseconds()))
		if evn == nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		switch tp := evn.(type) {
		case *kafka.Message:
			if tp == nil {
				coo.logger.Warn("tp is nil")
				continue
			}
			ctx := context.Background()

			yes, err := coo.commitState.LessThanStart(ctx, *tp.TopicPartition.Topic, tp.TopicPartition.Partition, tp.TopicPartition.Offset)
			if err != nil {
				coo.logger.Errorf("%+v", err)
			}
			if !yes {
				ctx = WithCheckUnique(ctx, false)
			}

			// coo.runController.Accept(ctx, tp)
		case *kafka.Error:
			coo.logger.Errorf("Error: %v: %v\n", tp.Code(), tp)
			time.Sleep(time.Second)
		}
	}
}

func (coo *Coordinator) Close() error {
	err := coo.consumer.Close()
	// coo.runController.Close()
	return err
}
