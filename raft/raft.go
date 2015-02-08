package raft

import (
	handler "github.com/swapniel99/cs733-raft/handler"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"encoding/gob"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
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
	ClusterConfig     *ClusterConfig
	ServerID          int
	CommitCh          chan LogEntry
	CommitIndex       Lsn
	LeaderCommitIndex Lsn

	//entries for implementing Shared Log
	LogEntryBuffer []LogEntry
	CurrentLsn     Lsn

	//channel for receving append requests
	AppendRequestChannel chan handler.AppendRequestMessage

	//state of the server
	CurrentState int
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
	raft := new(Raft)
	raft.ClusterConfig = config
	raft.ServerID = thisServerId
	raft.CommitCh = commitCh
	raft.AppendRequestChannel = make(chan handler.AppendRequestMessage)
	raft.LogEntryBuffer = make([]LogEntry, 0)
	return raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

//LogEntry interface implementation
type MyLogEntry struct {
	LogSeqNumber       Lsn
	DataBytes      []byte
	EntryCommitted bool
}

func (entry MyLogEntry) Lsn() Lsn {
	return entry.LogSeqNumber
}

func (entry MyLogEntry) Data() []byte {
	return entry.DataBytes
}

func (entry MyLogEntry) Committed() bool {
	return entry.EntryCommitted
}

func (raft *Raft) Append(data []byte) (LogEntry, error) {
	if len(data) > 0 {
		raft.CurrentLsn++
		var logEntry LogEntry
		logEntry = MyLogEntry{raft.CurrentLsn, data, false}
		raft.LogEntryBuffer = append(raft.LogEntryBuffer, logEntry)
		return logEntry, nil
	} else {
		return nil, new(ErrRedirect)
	}
}

//This struct will be sent as RPC message between the replicas
type RPCMessage struct {
	//TODO: add Term and previosLeaderLogTerm later
	LogEntry                     LogEntry
	LeaderID                     int //TODO: to be implemented later
	LeaderCommitIndex            Lsn
	PreviousLeaderSharedLogIndex Lsn
}

var ResponseChannelStore = struct {
	sync.RWMutex
	m map[Lsn]*chan string
}{m: make(map[Lsn]*chan string)}

//TODO: need to check the name of method here
func (raft *Raft) AppendEntriesRPC(message *RPCMessage, reply *bool) error {

	/*
		//case 1 - term is less than cyurrent term //TODO: to be implemented

		//case 2 - if follower contains entry @ previousLeaderLogIndex with same term then OK
		if raft.LogEntryBuffer[len(raft.LogEntryBuffer)-1].Lsn() >= message.previousLeaderSharedLogIndex {
			*reply = false
			return nil
		}

		//case 3 - same index but different Terms then delete that entry and further all log entries//TODO: to be implemented later

		//now append the new entry received
		raft.CurrentLsn++
		raft.LogEntryBuffer = append(raft.LogEntryBuffer, message.logEntry)

		//case 4 - if leaderCommitIndex > commitIndex then set commitIndex = min(leaderCommitIndex, raft.currentLsn)
		if message.leaderCommitIndex > raft.CommitIndex {
			raft.CommitIndex = Lsn(math.Min(float64(message.leaderCommitIndex), float64(raft.CurrentLsn)))
		}
	*/

	*reply = true
	return nil
}

func (raft *Raft) BroadcastMessageToReplicas(message *RPCMessage) bool {
	//TODO: contact all the replicas
	done := make(chan bool)

	for _, server := range raft.ClusterConfig.Servers {
		if server.Id == raft.ServerID {
			continue
		}
		//Make RPC call on  server.LogPort in a seperate go routine
		go func() {
			remoteServer, err := rpc.Dial("tcp", server.Hostname+":"+strconv.Itoa(server.LogPort))
			if err != nil {
				log.Println("Error Dialing: ", err)
			} else {

				// Synchronous call
				var reply bool
				err = remoteServer.Call("Raft.AppendEntriesRPC", message, &reply)
				if err != nil {
					log.Println("Error RPC call: ", err)
				}
				done <- reply
			}
		}()
	}

	//TODO: need to rectify the design as the go routine would block if it does not receive sufficeint acks
	ackCount := 0
	for ackCount < (len(raft.ClusterConfig.Servers) / 2) {
		if <-done {
			ackCount += 1
		}
	}
	return true
}

func (raft *Raft) StartServer() {
	log.Println("Started raft")
	//register for RPC
	rpc.Register(raft)
	gob.Register(MyLogEntry{})
	
	//start listening for RPC connections
	go raft.startRPCListener()

	//now start listening on the input channel from the connection handler for new append requests
	var message handler.AppendRequestMessage
	for {
		message = <-raft.AppendRequestChannel

		logEntry, err := raft.Append(message.Data)
		if err != nil {
			//TODO: this case would never happen
			*message.ResponseChannel <- "ERR_APPEND"
			continue
		}

		//put entry in the global map
		ResponseChannelStore.Lock()
		ResponseChannelStore.m[logEntry.Lsn()] = message.ResponseChannel
		ResponseChannelStore.Unlock()

		//now check for consensus
		rpcMessage := &RPCMessage{logEntry, raft.ServerID, raft.LeaderCommitIndex,
			raft.LogEntryBuffer[len(raft.LogEntryBuffer)-1].Lsn()}
		if raft.BroadcastMessageToReplicas(rpcMessage) {
			raft.CommitCh <- logEntry

			//TODO: commit the log entry
			//write to disk

			//update the commit index

		}

	}
}

func (raft *Raft) startRPCListener() {
	var logPort int
	for _, server := range raft.ClusterConfig.Servers {
		if server.Id == raft.ServerID {
			logPort = server.LogPort
		}
	}
	listener, e := net.Listen("tcp", ":"+strconv.Itoa(logPort))
	if e != nil {
		log.Fatal("Error starting RPC Listener: ", e.Error())
	}

	for {
		if conn, err := listener.Accept(); err != nil {
			log.Fatal("RPC connection accept error: " + err.Error())
		} else {
			log.Printf("New RPC connection accepted: ", conn.RemoteAddr().String())
			go rpc.ServeConn(conn)
		}
	}
}
