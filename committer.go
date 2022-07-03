package guiche

import (
	"fmt"
	"sync"

	"github.com/bookrun-go/guiche/glog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewCommitter(consumer *kafka.Consumer) *Committer {
	return &Committer{
		consumer:   consumer,
		partitions: make(map[string]*TopicPartiontionRecords, 128),
	}
}

type Committer struct {
	partitions map[string]*TopicPartiontionRecords
	consumer   *kafka.Consumer
	logger     glog.Logger
}

var count int

func (c Committer) AddTopicPartition(tp kafka.TopicPartition) {
	count++
	recordKey := fmt.Sprintf("%s-%d", *tp.Topic, tp.Partition)
	records, ok := c.partitions[recordKey]
	if !ok {
		records = new(TopicPartiontionRecords)
		c.partitions[recordKey] = records
	}
	c.logger.Infof("received topic:%s partition:%d offset:%d count:%d message", *tp.Topic, tp.Partition, tp.Offset, count)
	records.add(&TopicPartition{topicPartion: tp})
}

func (c *Committer) Finish(tps []kafka.TopicPartition) {
	if len(tps) == 0 {
		return
	}

	recordKey := fmt.Sprintf("%s-%d", *tps[0].Topic, tps[0].Partition)
	records, ok := c.partitions[recordKey]
	if !ok {
		return
	}

	commitOffset := records.FinishList(tps)

	if commitOffset.Topic != nil {
		c.logger.Debugf("commit finish%v", commitOffset)

		commitOffset.Offset++
		c.consumer.CommitOffsets([]kafka.TopicPartition{commitOffset})
	}
}

type TopicPartition struct {
	topicPartion kafka.TopicPartition
	Finish       bool
}

type TopicPartiontionRecords struct {
	lock    sync.Mutex
	records []*TopicPartition
}

func (cr *TopicPartiontionRecords) add(r *TopicPartition) {
	cr.lock.Lock()
	defer cr.lock.Unlock()

	cr.records = append(cr.records, r)
}

func (cr *TopicPartiontionRecords) FinishList(tps []kafka.TopicPartition) kafka.TopicPartition {
	cr.lock.Lock()
	defer cr.lock.Unlock()

	var lastTP kafka.TopicPartition
	for _, tp := range tps {
		temp := cr.finish(tp.Offset)
		if temp.Topic != nil {
			lastTP = temp
		}
	}

	return lastTP
}

func (cr *TopicPartiontionRecords) finish(offset kafka.Offset) kafka.TopicPartition {
	var commitOffset kafka.TopicPartition
	index := 0
	hasFind := false
	findCommit := true
	for i, r := range cr.records {
		if r.topicPartion.Offset == offset {
			r.Finish = true
			hasFind = true
		}

		if !r.Finish {
			findCommit = false
		}

		if findCommit {
			commitOffset = r.topicPartion
			index = i
		}

		if hasFind && !findCommit {
			break
		}
	}

	if index > 0 {
		cr.records = cr.records[index+1:]
	}

	return commitOffset
}
