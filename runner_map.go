package guiche

import "time"

type keyNode struct {
	latestTime time.Time
	runnerNode *RunnerNode
}

type KeyRunner map[string]*keyNode

func NewKeyRunner() KeyRunner {
	return make(KeyRunner)
}

func (kr KeyRunner) Find(key string) *keyNode {
	kn := kr[key]
	if kn == nil {
		return nil
	}

	kn.latestTime = time.Now()

	return kn
}

func (kr KeyRunner) Add(key string, rn *RunnerNode) *keyNode {
	kn, ok := kr[key]
	if ok {
		kr[key].latestTime = time.Now()
		return kn
	}
	
	kn = &keyNode{
		latestTime: time.Now(),
		runnerNode: rn,
	}

	kr[key] = kn

	return kn
}
