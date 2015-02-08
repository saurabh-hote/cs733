package main

import (
	"os/exec"
)

func main() {
	serverReplicas := 5
	programName := "server.go"
	command := "go run"
	index := 1
	for index <= serverReplicas {
		exec.Command(command, programName, string(index))
		index++
	}
}
