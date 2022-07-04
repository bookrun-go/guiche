package guiche

import (
	"context"
	"time"

	"github.com/bookrun-go/guiche/glog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Runner struct {
	// 外面传进来的
	newHandle  func() Handle
	logger     glog.Logger
	delKeyChan chan *delKey

	// 根据接收到的参数随时变化
	id     string
	handle Handle

	// 创建时初始化
	closedChane chan bool
	msgChan     chan *KMessage
}

func NewRunner(newHandle func() Handle, logger glog.Logger, delKeyChan chan *delKey) *Runner {
	r := &Runner{
		newHandle:  newHandle,
		logger:     logger,
		delKeyChan: delKeyChan,

		closedChane: make(chan bool),
		msgChan:     make(chan *KMessage, 100),
	}

	go r.run()

	return r
}

func (r *Runner) Accept(ctx context.Context, msg *kafka.Message) {
	r.msgChan <- &KMessage{ctx: ctx, msg: msg}
}

func (r *Runner) Close(ctx context.Context) {
	close(r.msgChan)
	<-r.closedChane
}

func (r *Runner) run() {
	defer func() {
		r.closedChane <- true
	}()
	isEnd := false
	for msg := range r.msgChan {
		if msg == nil {
			r.logger.Warn("msg is nil")
			continue
		}

		id := string(msg.msg.Key)
		if id != r.id {
			if r.handle != nil { // 关闭前一个handle
				err := r.handle.Close()
				if err != nil {
					r.logger.Errorf("close handle err, key[%v] err:%+v", msg.msg.Key, err)
				}
			}

			r.delKeyChan <- &delKey{key: id, delTime: time.Now()}
			r.handle = r.newHandle()
			r.id = id
		}

		end, err := r.handle.Accept(msg.ctx, msg.msg.Key, msg.msg.Value)
		if err != nil {
			r.logger.Errorf("handle err, key[%v] err:%+v", msg.msg.Key, err)
		}
		isEnd = end

		if end {
			err = r.handle.Close()
			if err != nil {
				r.logger.Errorf("close handle err, key[%v] err:%+v", msg.msg.Key, err)
			}
			return
		}
	}

	if !isEnd && r.handle != nil {
		err := r.handle.Close()
		if err != nil {
			r.logger.Errorf("close handle err, key[%v] err:%+v", r.id, err)
		}
	}
}
