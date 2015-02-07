package raft

import (
	"encoding/json"
	handler "github.com/swapniel99/cs733-raft/handler"
	"io/ioutil"
	"log"
	"math"
	"net/rpc"
	"os"
	"strconv"
)

type Lsn uint64      //Log sequence number, unique for all time.
type ErrRedirect int // See Log.Append. Implements Error interface.
type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data []byte) (LogEntry, error)
}

// --------------------------------------
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

// Raft implements the SharedLog interface.
type Raft struct {
	clusterConfig     *ClusterConfig
	serverID          int
	commitCh          chan LogEntry
	commitIndex       Lsn
	leaderCommitIndex Lsn

	//entries for implementing Shared Log
	logEntryBuffer []LogEntry
	currentLsn     Lsn
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
	raft := new(Raft)
	raft.clusterConfig = config
	raft.serverID = thisServerId
	raft.commitCh = commitCh
	return raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

//LogEntry interface implementation
type MyLogEntry struct {
	lsn       Lsn
	data      []byte
	committed bool
}

func (entry MyLogEntry) Lsn() Lsn {
	return entry.lsn
}

func (entry MyLogEntry) Data() []byte {
	return entry.data
}

func (entry MyLogEntry) Committed() bool {
	return entry.committed
}

func (raft Raft) Append(data []byte) (LogEntry, error) {
	if len(data) > 0 {
		raft.currentLsn++
		logEntry := MyLogEntry{raft.currentLsn, data, false}
		raft.logEntryBuffer = append(raft.logEntryBuffer, logEntry)
		return logEntry, nil
	} else {
		return nil, new(ErrRedirect)
	}
}

//This struct will be sent as RPC message between the replicas
type RPCMessage struct {
	//TODO: add Term and previosLeaderLogTerm later
	logEntry                     LogEntry
	leaderID                     int //TODO: to be implemented later
	leaderCommitIndex            Lsn
	previousLeaderSharedLogIndex Lsn
}

//TODO: need to check the name of method here
func (raft *Raft) AppendEntriesRPC(message *RPCMessage, reply *bool) error {

	//case 1 - term is less than cyurrent term //TODO: to be implemented

	//case 2 - if follower contains entry @ previousLeaderLogIndex with same term then OK
	if raft.logEntryBuffer[len(raft.logEntryBuffer)-1].Lsn() >= message.previousLeaderSharedLogIndex {
		*reply = false
		return nil
	}

	//case 3 - same index but different Terms then delete that entry and further all log entries//TODO: to be implemented later

	//now append the new entry received
	raft.currentLsn++
	raft.logEntryBuffer = append(raft.logEntryBuffer, message.logEntry)

	//case 4 - if leaderCommitIndex > commitIndex then set commitIndex = min(leaderCommitIndex, raft.currentLsn)
	if message.leaderCommitIndex > raft.commitIndex {
		raft.commitIndex = Lsn(math.Min(float64(message.leaderCommitIndex), float64(raft.currentLsn)))
	}

	*reply = true
	return nil
}

func (raft *Raft) BroadcastMessageToReplicas(message *RPCMessage) bool {
	//TODO: contact all the replicas
	done := make(chan bool)

	for _, server := range raft.clusterConfig.Servers {
		if server.Id == raft.serverID {
			continue
		}
		//Make RPC call on  server.LogPort in a seperate go routine
		go func() {
			remoteServer, err := rpc.Dial("tcp", server.Hostname+":"+strconv.Itoa(server.LogPort))
			if err != nil {
				log.Fatal("Dialing: ", err)
			}

			// Synchronous call
			var reply bool
			err = remoteServer.Call("Raft.AppendEntriesRPC", message, &reply)
			if err != nil {
				log.Fatal("RPC call: ", err)
			}
			done <- reply
		}()
	}

	//TODO: need to rectify the design as the go routine would block if it does not receive sufficeint acks
	ackCount := 0
	for ackCount < (len(raft.clusterConfig.Servers)/2 + 1) {
		if <-done {
			ackCount += 1
		}
	}
	return true
}

func StartServer(raft Raft) {
	//register for RPC
	rpc.Register(raft)

	//start kvstore module
	go Initialize(raft.commitCh)

	//start conenction handler module
	go raft.InitializeConnectionHandler()
}

//This function is called by the ConnectionHandler module upon receiving command from the client
func (raft *Raft) UpdateSharedLog(data []byte) {
	logEntry, err := raft.Append(data)
	if err != nil {
		//TODO: this case would never happen
	}

	rpcMessage := &RPCMessage{logEntry, raft.serverID, raft.leaderCommitIndex,
		raft.logEntryBuffer[len(raft.logEntryBuffer)-1].Lsn()}
	if raft.BroadcastMessageToReplicas(rpcMessage) {
		//TODO: commit the log entry
		//write to disk

		//update the commit index

	}
}

func ReadConfig(path string) (*ClusterConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf ClusterConfig
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
	var ch chan LogEntry
	raft, err := NewRaft(clusterConfig, serverID, ch)
	if err != nil {
		log.Println("Error creating server instance : ", err.Error())
	}
	StartServer(*raft)
}

func (raft *Raft) InitializeConnectionHandler() {
	var clientPort int
	for _, server := range raft.clusterConfig.Servers {
		if server.Id == raft.serverID {
			clientPort = server.ClientPort
		}
	}
	if clientPort == 0 {
		log.Println("Server's client port not valid")
	} else {
		handler.StartConnectionHandler(clientPort)
	}
}
