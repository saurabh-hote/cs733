package raft

import (
	"log"
	"math"
	"net/rpc"
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
	serverConfig      *ServerConfig
	commitCh          chan LogEntry
	mySharedLog       MySharedLog
	commitIndex       Lsn
	leaderCommitIndex Lsn
	outbox            chan LogEntry
	inbox             chan LogEntry
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
	return nil, nil
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

//SharedLog interface implementation
type MySharedLog struct {
	logEntryBuffer []LogEntry
	currentLsn     Lsn
}

func (mySharedLog MySharedLog) Append(data []byte) (LogEntry, error) {
	if len(data) > 0 {
		mySharedLog.currentLsn++
		logEntry := MyLogEntry{mySharedLog.currentLsn, data, false}
		mySharedLog.logEntryBuffer = append(mySharedLog.logEntryBuffer, logEntry)
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

func (raft *Raft) AppendEntryRPC(message *RPCMessage, reply *bool) error {

	//case 1 - term is less than cyurrent term //TODO: to be implemented

	//case 2 - if follower contains entry @ previousLeaderLogIndex with same term then OK
	if raft.mySharedLog.logEntryBuffer[len(raft.mySharedLog.logEntryBuffer)-1].Lsn() >= message.previousLeaderSharedLogIndex {
		*reply = false
		return nil
	}

	//case 3 - same index but different Terms then delete that entry and further all log entries//TODO: to be implemented later

	//now append the new entry received
	raft.mySharedLog.currentLsn++
	raft.mySharedLog.logEntryBuffer = append(raft.mySharedLog.logEntryBuffer, message.logEntry)

	//case 4 - if leaderCommitIndex > commitIndex then set commitIndex = min(leaderCommitIndex, raft.mySharedLog.currentLsn)
	if message.leaderCommitIndex > raft.commitIndex {
		raft.commitIndex = Lsn(math.Min(float64(message.leaderCommitIndex), float64(raft.mySharedLog.currentLsn)))
	}

	*reply = true
	return nil
}

func (raft *Raft) broadcastMessageToReplicas(message *RPCMessage) bool {
	//TODO: contact all the replicas
	done := make(chan bool)

	for _, server := range raft.clusterConfig.Servers {
		//Make RPC call on  server.LogPort in a seperate go routine
		go func() {
			remoteServer, err := rpc.Dial("tcp", server.Hostname+":"+strconv.Itoa(server.LogPort))
			if err != nil {
				log.Fatal("Dialing: ", err)
			}

			// Synchronous call
			var reply bool
			err = remoteServer.Call("Raft.AppendRPC", message, &reply)
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

func startServer(serverConfig ServerConfig, clusterConfig ClusterConfig) {
	raft := new(Raft)
	rpc.Register(raft)
	raft.serverConfig = &serverConfig
	raft.clusterConfig = &clusterConfig
	raft.commitCh = make(chan LogEntry, 10) //TODO: change harcoding

	//TODO: start conenction handler module

	//TODO: start kvstore module
}

//This function is called by the ConnectionHandler module upon receiving command from the client
func (raft *Raft) updateSharedLog(data []byte) {
	logEntry, err := raft.mySharedLog.Append(data)
	if err != nil {
		//TODO: this case would never happen
	}

	rpcMessage := &RPCMessage{logEntry, raft.serverConfig.Id, raft.leaderCommitIndex, raft.mySharedLog.logEntryBuffer[len(raft.mySharedLog.logEntryBuffer)-1].Lsn()}
	if raft.broadcastMessageToReplicas(rpcMessage) {
		//TODO: commit the log entry
		//write to disk

		//update the commit index

	}
}

func main() {
	//TODO: Read the config.json file to get all the server configurations
	servers := []ServerConfig{
		{1, "localhost", 9000, 10000}, // {id, hostname, clientPort, logPort}
		{2, "localhost", 9001, 10001},
		{3, "localhost", 9002, 10002},
		{4, "localhost", 9003, 10003},
		{5, "localhost", 9004, 10004},
	}
	clusterConfig := ClusterConfig{"/log", servers}

	//starting all the server replicas
	for _, serverConfig := range servers {
		go startServer(serverConfig, clusterConfig)
	}
}