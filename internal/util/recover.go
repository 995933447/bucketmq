package util

import (
	"fmt"
	"runtime/debug"
	"strings"
)

func StackRecover() {
	if err := recover(); err != nil {
		stack := debug.Stack()
		if len(stack) > 0 {
			lines := strings.Split(string(stack), "\n")
			for _, line := range lines {
				fmt.Println(line)
				Logger.Fatalf(nil, line)
			}
			return
		}
		notStackMsg := fmt.Sprintf("stack is empty (%s)\n", err)
		Logger.Fatalf(nil, notStackMsg)
		fmt.Printf(notStackMsg)
	}
}
