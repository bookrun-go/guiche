package guiche

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type RunnerController struct {
	newRunner  func() *Runner
	runnerList []*Runner
	runnerMap  map[string]int
	closeList  []string

	lock sync.RWMutex
}

func (controller *RunnerController) Accept(ctx context.Context, msg *kafka.Message) {
	controller.lock.RLocker()
	defer controller.lock.RUnlock()

	controller.findRunner(msg).Accept(ctx, msg)
}

func (controller *RunnerController) findRunner(msg *kafka.Message) *Runner {
	index, ok := controller.runnerMap[string(msg.Key)]
	if ok {
		return controller.runnerList[index]
	}

	controller.runnerList = append(controller.runnerList, controller.newRunner())
	controller.runnerMap[string(msg.Key)] = len(controller.runnerList) - 1

	return nil
}

func (controller *RunnerController) AsyncRemoveClosedRunner() {
	tc := time.NewTicker(time.Second)
	for range tc.C {
		controller.removeClosedRunner()
	}
}

func (controller *RunnerController) removeClosedRunner() {
	controller.lock.Lock()
	defer controller.lock.Unlock()

	for _, id := range controller.closeList {
		index, ok := controller.runnerMap[id]
		if !ok {
			continue
		}
		runner := controller.runnerList[index]
		if runner.id == id {
			continue
		}

		delete(controller.runnerMap, id)
	}

	controller.closeList = nil
}
