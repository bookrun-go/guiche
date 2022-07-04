package guiche

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

type delMap2 map[string]time.Time

func (del delMap2) add(key string) {
	(del)[key] = time.Now()
}

func (del delMap2) remove(key string) {
	delete(del, key)
}
func (del *delMap2) clean() {
	*del = make(delMap2)
}

func (del *delMap2) clean2() {
	if del == nil {
		fmt.Println("is nil")
	}
}

func TestMapPointer(t *testing.T) {
	dm1 := make(delMap2, 2)

	for i := 0; i < 10000; i++ {
		dm1.add(strconv.FormatInt(int64(i), 10))
	}

	dm1.add("110")
	dm1.remove("110")

	dm1.clean()
	dm1.add("110")

	var dm2 delMap2

	dm2.clean2()

}
