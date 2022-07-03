package guiche

import "context"

type Handle interface {
	Accept(context.Context, []byte, []byte) (end bool, _ error)
	Close() error
}
