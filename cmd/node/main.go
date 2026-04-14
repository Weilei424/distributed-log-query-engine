package main

import (
	"fmt"
	"os"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node-local"
	}
	fmt.Printf("node starting: id=%s\n", nodeID)
}
