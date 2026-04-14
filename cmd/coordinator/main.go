package main

import (
	"fmt"
	"os"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "coordinator-local"
	}
	fmt.Printf("coordinator starting: id=%s\n", nodeID)
}
