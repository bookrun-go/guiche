package guiche

type RunnerNode struct {
	pre   *RunnerNode
	next  *RunnerNode
	this  *Runner
	quote map[string]bool
}

func (rn *RunnerNode) AddQuote(key string) {
	rn.quote[key] = true
}

func (rn *RunnerNode) RemoveQuote(key string) {
	delete(rn.quote, key)
}

type RunnerRing struct {
	startNode *RunnerNode
	count     int
}

func NewRunnerRing() *RunnerRing {
	return &RunnerRing{}
}

func (ring *RunnerRing) TailAdd(runner *Runner) *RunnerNode {
	if ring.startNode == nil {
		return ring.init(runner)
	}

	ring.count++

	newNode := &RunnerNode{
		this:  runner,
		pre:   ring.startNode.pre,
		next:  ring.startNode,
		quote: make(map[string]bool),
	}

	ring.startNode.pre.next = newNode
	ring.startNode.pre = newNode

	return newNode
}

func (ring *RunnerRing) init(runner *Runner) *RunnerNode {
	ring.count++

	ring.startNode = &RunnerNode{
		this:  runner,
		quote: make(map[string]bool),
	}
	ring.startNode.next = ring.startNode
	ring.startNode.pre = ring.startNode

	return ring.startNode
}

func (ring *RunnerRing) Remove(rn *RunnerNode) {
	if rn.pre == nil || rn.next == nil {
		return
	}

	// 校验合法性
	if rn.pre.next != rn || rn.next.pre != rn {
		return
	}

	ring.count--

	rn.pre.next = rn.next
	rn.next.pre = rn.pre
}

func (ring *RunnerRing) Roll() *RunnerNode {
	if ring.startNode == nil {
		return nil
	}

	rn := ring.startNode
	ring.startNode = rn.next
	return rn
}
