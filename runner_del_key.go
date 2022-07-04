package guiche

import "time"

type delKey struct {
	key     string
	delTime time.Time
}

type delKeyMap map[string]time.Time

func NewDelMap() delKeyMap {
	return make(delKeyMap)
}

func (del delKeyMap) Add(dk *delKey) {
	oldTime, ok := del[dk.key]
	if ok && dk.delTime.Before(oldTime) {
		return
	}

	del[dk.key] = dk.delTime
}

func (del delKeyMap) Each(f func(k string, date time.Time)) {
	for k, v := range del {
		f(k, v)
	}
}

func (del *delKeyMap) Clean() {
	*del = make(delKeyMap)
}
