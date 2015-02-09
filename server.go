package main

import (
	"encoding/json"
	handler "github.com/swapniel99/cs733-raft/handler"
	raft "github.com/swapniel99/cs733-raft/raft"
	"io/ioutil"
	"log"
	"flag"
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

	//starting the server
	serverIdPtr := flag.Int("id", 1, "an int")
	flag.Parse()
	
	if err != nil {
		log.Println("Invalid Server ID provided : ", err.Error())
	}
	log.Println("Starting sevrer with ID ", *serverIdPtr)

	commitCh := make(chan raft.LogEntry, 10000)
	raftInstance, err := raft.NewRaft(clusterConfig, *serverIdPtr, commitCh)
	if err != nil {
		log.Println("Error creating server instance : ", err.Error())
	}

	//Server with ID 1 will be the default leader
	if *serverIdPtr == 1 {
			raftInstance.CurrentState = raft.LEADER
	} else {
		raftInstance.CurrentState = raft.FOLLOWER
	}

	//Initialize the connection handler module
	var clientPort int
	for _, server := range (*raftInstance).ClusterConfig.Servers {
		if server.Id == (*raftInstance).ServerID {
			clientPort = server.ClientPort
		}
	}
	if clientPort <= 0 {
		log.Println("Server's client port not valid")
	} else {
		go handler.StartConnectionHandler(clientPort, raftInstance.AppendRequestChannel)
	}

	//Inititialize the KV Store Module
	go raft.InitializeKVStore(raftInstance.CommitCh)

	//Now start the SharedLog module
	raftInstance.StartServer()
	
	log.Println("Started raft Instance for server ID ", raftInstance.ServerID)
}
