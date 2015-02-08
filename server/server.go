package main

import (
	"encoding/json"
	handler "github.com/swapniel99/cs733-raft/handler"
	"io/ioutil"
	"os"
	raft "github.com/swapniel99/cs733-raft/raft"
	"log"
	"strconv"
)

func ReadConfig(path string) (*raft.ClusterConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf raft.ClusterConfig
	err = json.Unmarshal(data, &conf)
	return &conf, err
}

func main() {
	//TODO: Read the config.json file to get all the server configurations
	clusterConfig, err := ReadConfig("config.json")
	if err != nil {
		log.Println("Error parsing config file : ", err.Error())
	}

	/*
		servers := []ServerConfig{
			{1, "localhost", 9000, 10000}, // {id, hostname, clientPort, logPort}
			{2, "localhost", 9001, 10001},
			{3, "localhost", 9002, 10002},
			{4, "localhost", 9003, 10003},
			{5, "localhost", 9004, 10004},
		}

	*/
	//starting the server
	serverID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Println("Invalid Server ID provided : ", err.Error())
	}
	var ch chan raft.LogEntry
	raftInstance, err := raft.NewRaft(clusterConfig, serverID, ch)
	if err != nil {
		log.Println("Error creating server instance : ", err.Error())
	}
	InitializeConnectionHandler(raftInstance)
	raftInstance.StartServer()
}

func InitializeConnectionHandler(raftInstance *raft.Raft) {
	var clientPort int
	for _, server := range (*raftInstance).ClusterConfig.Servers {
		if server.Id == (*raftInstance).ServerID {
			clientPort = server.ClientPort
		}
	}
	if clientPort == 0 {
		log.Println("Server's client port not valid")
	} else {
		var appendRequestChannel chan handler.AppendRequestMessage 
		(*raftInstance).AppendRequestChannel = appendRequestChannel
		handler.StartConnectionHandler(clientPort, appendRequestChannel)
	}
}
