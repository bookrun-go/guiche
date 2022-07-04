package inner

import (
	"context"
	"fmt"
	_ "unsafe"
)

//go:linkname printName github.com/bookrun-go/guiche/mmmlinkname/outer.pName112
func printName(ctx context.Context, age int) {
	fmt.Println("name", age)
}
