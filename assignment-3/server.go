package main

import (
	"encoding/json"
	"errors"
	handler "github.com/saurabh-hote/cs733/assignment-3/handler"
	util "github.com/saurabh-hote/cs733/assignment-3/util"
	raft "github.com/saurabh-hote/cs733/assignment-3/raft"
	"io/ioutil"
	"log"
	"sync"
)

type NullWriter int

func (NullWriter) Write([]byte) (int, error) {
	return 0, nil
}

var wg sync.WaitGroup
var clusterConfig *raft.ClusterConfig

const noOfClientThreads int = 10
const noOfRequestsPerThread int = 10

var sharedEventChannelMap map[int]*chan util.Event

func main() {
	serverConfig := []raft.ServerConfig{
		{1, "localhost", 5001, 6001},
		{2, "localhost", 5002, 6002},
		{3, "localhost", 5003, 6003},
		{4, "localhost", 5004, 6004},
		{5, "localhost", 5005, 6005}}
	clusterConfig = &raft.ClusterConfig{"/log", serverConfig}

	data, _ := json.Marshal(*clusterConfig)
	ioutil.WriteFile("config.json", data, 0644)

	//create the sahred channels
	createSharedChannels()

	serverReplicas := len((*clusterConfig).Servers)
	index := 1
	for index <= serverReplicas {
		constIndex := index
		log.Println("Starting server with ID = ", constIndex)
		wg.Add(1)
		go Start(constIndex)
		index++
	}
	log.Println("Started all the server replicas..")
	wg.Wait()
	log.Println("Stopped all the server replicas..")
}

func ReadConfig(path string) (*raft.ClusterConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf raft.ClusterConfig
	err = json.Unmarshal(data, &conf)
	if err == nil && len(conf.Servers) < 1 {
		err = errors.New("No Server Configuration found")
	}
	return &conf, err
}

func Start(serverID int) {
	defer wg.Done()

	//log.SetOutput(new(NullWriter))

	//TODO: Read the config.json file to get all the server configurations
	clusterConfig, err := ReadConfig("config.json")
	if err != nil {
		log.Println("Error parsing config file : ", err.Error())
		return
	}

	//starting the server

	commitCh := make(chan util.LogEntry, 100)
	raftInstance, err := raft.NewRaft(clusterConfig, serverID, commitCh)
	if err != nil {
		log.Println("Error creating server instance : ", err.Error())
	}
	
	//Set the shared event map to the raft instance
	raftInstance.SharedEventChannelMap  = &sharedEventChannelMap
	
	raftInstance.EventInCh = *sharedEventChannelMap[serverID]
	//First entry in the ClusterConfig will be the default leader
	var clientPort int
	for _, server := range raftInstance.ClusterConfig.Servers {
		//Initialize the connection handler module
		if server.Id == raftInstance.ServerID {
			clientPort = server.ClientPort
			raftInstance.CurrentState = raft.FOLLOWER
		}
	}

	if clientPort <= 0 {
		log.Println("Server's client port not valid")
	} else {
		go handler.StartConnectionHandler(clientPort, raftInstance.EventInCh)
	}

	//Inititialize the KV Store Module
	go raft.InitializeKVStore(raftInstance.CommitCh)

	//Initialize the server 
	raftInstance.InitServer()

	//Now start the Loop
	raftInstance.Loop()

	log.Println("Started raft Instance for server ID ", raftInstance.ServerID)
}

func createSharedChannels() {
	sharedEventChannelMap = make(map[int]*chan util.Event)
	for _, server := range clusterConfig.Servers {
		channel := make(chan util.Event, 1000)
		sharedEventChannelMap[server.Id] = &channel
	}
}
