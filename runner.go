package guiche

import (
	"context"

	"github.com/bookrun-go/guiche/glog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Runner struct {
	id        string
	msgChan   chan *KMessage
	handle    Handle
	newHandle func() Handle
	logger    glog.Logger

	closed bool
}

func (r *Runner) Accept(ctx context.Context, msg *kafka.Message) {
	r.msgChan <- &KMessage{ctx: ctx, msg: msg}
}

func (r *Runner) IsClosed(ctx context.Context) bool {
	return r.closed
}

func (r *Runner) Close(ctx context.Context) {
	close(r.msgChan)
}

func (r *Runner) run() {
	defer func() {
		r.closed = true
	}()

	isEnd := false
	for msg := range r.msgChan {
		if msg == nil {
			r.logger.Warn("msg is nil")
			continue
		}

		if string(msg.msg.Key) != r.id {
			if r.handle != nil { // 关闭前一个handle
				err := r.handle.Close()
				if err != nil {
					r.logger.Errorf("close handle err, key[%v] err:%+v", msg.msg.Key, err)
				}
			}

			r.handle = r.newHandle()
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
