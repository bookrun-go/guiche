package guiche

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type RunnerController struct {
	// 外面传过来的
	newRunner       func() *Runner
	listenCloseChan chan *delKey
	maxRunner       int

	// 内部初始化
	runnerRing *RunnerRing
	runnerMap  KeyRunner
	delMap     delKeyMap

	lock sync.RWMutex
}

func NewRunnerController(newRunner func() *Runner, delKeyChan chan *delKey, maxRunner int) *RunnerController {
	rc := &RunnerController{
		newRunner:       newRunner,
		runnerRing:      NewRunnerRing(),
		runnerMap:       NewKeyRunner(),
		listenCloseChan: delKeyChan,
		delMap:          NewDelMap(),
		maxRunner:       maxRunner,
	}

	go rc.listenClose()

	return rc
}

func (controller *RunnerController) Accept(ctx context.Context, msg *kafka.Message) {
	controller.lock.RLocker()
	defer controller.lock.RUnlock()

	key := string(msg.Key)

	r := controller.findRunner(ctx, key)

	r.Accept(ctx, msg)
}

func (controller *RunnerController) findRunner(ctx context.Context, key string) *Runner {
	r, ok := controller.runnerMap[key]
	if ok {
		r.latestTime = time.Now()
		return r.runnerNode.this
	}

	if controller.runnerRing.count >= controller.maxRunner {
		rn := controller.runnerRing.Roll()
		if rn == nil {
			panic("runner ring is nil")
		}

		rn.AddQuote(key)
		controller.runnerMap[key] = &keyNode{latestTime: time.Now(), runnerNode: rn}

		return rn.this
	}

	rn := controller.runnerRing.TailAdd(controller.newRunner())
	rn.AddQuote(key)
	controller.runnerMap[key] = &keyNode{latestTime: time.Now(), runnerNode: rn}

	return rn.this
}

func (controller *RunnerController) cleanDeletedKey() {
	controller.lock.Lock()
	defer controller.lock.Unlock()

	controller.delMap.Each(
		func(k string, date time.Time) {
			kr := controller.runnerMap[k]
			if kr == nil {
				return
			}

			if date.Before(kr.latestTime) {
				return
			}

			delete(controller.delMap, k)
			delete(controller.runnerMap, k)
			kr.runnerNode.RemoveQuote(k)

			if len(kr.runnerNode.quote) == 0 {
				controller.runnerRing.Remove(kr.runnerNode)
			}
		},
	)

	controller.delMap.Clean()
}

func (controller *RunnerController) listenClose() {
	for dk := range controller.listenCloseChan {
		controller.lock.RLock()
		controller.delMap.Add(dk)
		controller.lock.RUnlock()

		// 定期清理
		if len(controller.delMap) > 100 {
			controller.cleanDeletedKey()
		}
	}
}

func (controller *RunnerController) Close() {
	controller.lock.Lock()
	defer controller.lock.Unlock()

	startNode := controller.runnerRing.Roll()
	if startNode == nil {
		return
	}

	ctx := context.Background()
	startNode.this.Close(ctx)

	for rn := controller.runnerRing.Roll(); rn != startNode; {
		rn.this.Close(ctx)
	}
}
