package main

import (
	"fmt"
	"os"

	"opensearch-balanser/internal/docgate"
)

func main() {
	if err := docgate.CheckRepo("."); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	fmt.Println("documentation gate: PASS")
}
