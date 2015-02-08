package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
)

// Raft setup
type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

func getClusterConfig() ClusterConfig {
	serverConfig := []ServerConfig{
		{1, "localhost", 5001, 6001},
		{2, "localhost", 5002, 6002},
		{3, "localhost", 5003, 6003},
		{4, "localhost", 5004, 6004},
		{5, "localhost", 5005, 6005}}
	clusterConfig := ClusterConfig{"/log", serverConfig}
	return clusterConfig
}

func main() {
	clusterConfig := getClusterConfig()
	data, _ := json.Marshal(clusterConfig)
	ioutil.WriteFile("config.json", data, 0644)

	serverReplicas := 5
	programName := "server.go"
	index := 1
	for index <= serverReplicas {
		constIndex := index

		go func() {
			cmd := exec.Command("go", "run", programName, "-id="+strconv.Itoa(constIndex))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			if err != nil {
				log.Println("Server stopped :", constIndex, err.Error())
			}
		}()
		index++
	}
	fmt.Scanln()
}
