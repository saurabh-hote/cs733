package raft

import (
	"encoding/gob"
	util "github.com/saurabh-hote/cs733/assignment-3/util"

	"log"
	//	"net"
	//	"net/rpc"
	"strconv"
	"sync"
	//	"sync/atomic"
	"math/rand"
	"time"
)

//Constansts indicating the replica's state
const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

const (
	minTimeout = 50 //in seconds
	maxTimeout = 100
)

type ErrRedirect int // See Log.Append. Implements Error interface.
type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data []byte) (util.LogEntry, error)
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
	CommitCh          chan util.LogEntry
	CommitIndex       util.Lsn
	LeaderCommitIndex util.Lsn
	LeaderID          int
	CurrentState      string //state of the server

	//entries for implementing Shared Log
	LogEntryBuffer       []util.LogEntry
	CurrentLsn           util.Lsn
	EventInCh            chan util.Event
	ServerQuitCh         chan chan struct{}
	ElectionTimer        <-chan time.Time
	Term                 uint64
	LastVotedTerm        uint64
	LastVotedCandidateID int

	//TODO: To be removed with the socket implementation
	SharedEventChannelMap *map[int]*chan util.Event
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan util.LogEntry) (*Raft, error) {
	raft := new(Raft)
	raft.ClusterConfig = config
	raft.ServerID = thisServerId
	raft.CommitCh = commitCh
	//	raft.AppendRequestCh = make(chan handler.AppendRequestMessage)
	//	raft.LogEntryBuffer = make([]LogEntry, 0)

	return raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

//LogEntry interface implementation
type LogEntryObj struct {
	LogSeqNumber   util.Lsn
	DataBytes      []byte
	EntryCommitted bool
}

var ResponseChannelStore = struct {
	sync.RWMutex
	m map[util.Lsn]*chan string
}{m: make(map[util.Lsn]*chan string)}

func (entry LogEntryObj) Lsn() util.Lsn {
	return entry.LogSeqNumber
}

func (entry LogEntryObj) Data() []byte {
	return entry.DataBytes
}

func (entry LogEntryObj) Committed() bool {
	return entry.EntryCommitted
}

func (raft *Raft) Append(data []byte) (util.LogEntry, error) {
	if len(data) > 0 {
		raft.CurrentLsn++
		var logEntry util.LogEntry
		logEntry = LogEntryObj{raft.CurrentLsn, data, false}
		raft.LogEntryBuffer = append(raft.LogEntryBuffer, logEntry)
		return logEntry, nil
	} else {
		return nil, new(ErrRedirect)
	}
}

//TODO: need to check the name of method here
func (raft *Raft) AppendEntriesRPC(message *util.AppendEntryRequest, reply *bool) error {

	/*
		//case 1 - term is less than cyurrent term //TODO: to be implemented

		//case 2 - if follower contains entry @ previousLeaderLogIndex with same term then OK
		if raft.LogEntryBuffer[len(raft.LogEntryBuffer)-1].Lsn() >= message.LeaderPreviousSharedLogIndex {
			*reply = false
			return nil
		}

		//case 3 - same index but different Terms then delete that entry and further all log entries//TODO: to be implemented later

		//now append the new entry received
		raft.CurrentLsn++
		raft.LogEntryBuffer = append(raft.LogEntryBuffer, message.LogEntry)

		//case 4 - if leaderCommitIndex > commitIndex then set commitIndex = min(leaderCommitIndex, raft.currentLsn)
		if message.LeaderCommitIndex > raft.CommitIndex {
			raft.CommitIndex = Lsn(math.Min(float64(message.LeaderCommitIndex), float64(raft.CurrentLsn)))
		}
	*/

	*reply = true
	return nil
}

func (raft *Raft) Loop() {
	raft.CurrentState = FOLLOWER // begin life as a follower
	for {
		log.Println("Server", raft.ServerID, "in term", raft.Term, "in state ", raft.CurrentState)

		switch raft.CurrentState {
		case FOLLOWER:
			raft.CurrentState = raft.Follower()
		case CANDIDATE:
			raft.CurrentState = raft.Candidate()
		case LEADER:
			raft.CurrentState = raft.Leader()
		default:
			log.Println("Error: Unknown server state")
			return
		}
	}
}

func (raft *Raft) ResetTimer() *time.Timer {
	//Perfrom random selection for timeout peroid

	randomGenerator := rand.Intn(int(maxTimeout-minTimeout)) + (maxTimeout - minTimeout)
	timer := time.NewTimer(time.Duration(randomGenerator) * time.Second)
	go func() {
		<-timer.C
		//Time is expired - push Timeout event on the channel
		raft.EventInCh <- util.Event{util.TypeTimeout, util.Timeout{}}
	}()
	return timer
}

func (raft *Raft) Follower() string {
	//start timer // to become candidate if no append reqs
	timer := raft.ResetTimer()

	for {
		event := <-raft.EventInCh
		switch event.Type {
		case util.TypeClientAppendRequest:
			// Do not handle clients in follower mode. Send it back up the
			// pipe with committed = false
			message := event.Data.(util.ClientAppendRequest)
			leaderConfig := raft.ClusterConfig.Servers[0]
			for _, server := range raft.ClusterConfig.Servers {
				if server.Id == raft.LeaderID {
					leaderConfig = server
				}
			}

			var responseMsg string
			if leaderConfig.Id != raft.LeaderID {
				responseMsg = "ERR_REDIRECT " + "No Leader selected."
			} else {
				responseMsg = "ERR_REDIRECT " + leaderConfig.Hostname + " " + strconv.Itoa(leaderConfig.ClientPort) + "\r\n"

			}
			(*message.ResponseCh) <- responseMsg

		case util.TypeVoteRequest:
			message := event.Data.(util.VoteRequest)

			voteResp, _ := raft.validateVoteRequest(message)
			raft.sendToServerReplica(util.Event{util.TypeVoteReply, voteResp}, message.CandidateID)

			/*
				if message.Term < raft.Term {

				} //TODO: saurabh - add response

				if message.Term > raft.Term {
					raft.Term = message.Term
				}

				if message.Term != raft.LastVotedTerm {
					timer.Stop()
					timer = raft.ResetTimer()
					 reply ok to event.msg.serverid
					raft.sendToServerReplica(util.Event{util.TypeVoteReply, util.VoteReply{message.Term, true}}, message.CandidateID)
					//TODO: Saurabh - remember term, leader id (either in log or in separate file)
					raft.LastVotedTerm = message.Term
				}
			*/
		case util.TypeAppendEntryRequest:
			timer.Stop()
			timer = raft.ResetTimer()
			message := event.Data.(util.AppendEntryRequest)

			//TODO: change the code to incorporate Raft logic
			raft.sendToServerReplica(util.Event{util.TypeAppendEntryResponse, true}, message.LeaderID)

			/*

				if message.Term < raft.Term {
					continue
				}
				   reset heartbeat timer
				   upgrade to event.msg.term if necessary
				   if prev entries of my log and event.msg match
				      add to disk log
				      flush disk log
				      respond ok to event.msg.serverid
				   else
				      respond err.
			*/
		case util.TypeTimeout:
			log.Println("Election timed out. State for server ", raft.ServerID, " changing to ", CANDIDATE)
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			timer.Stop()
			timer = raft.ResetTimer()
			return CANDIDATE // new state back to loop()
		}

	}
	return raft.CurrentState
}

func (raft *Raft) Candidate() string {
	//start timer // to become candidate if no append reqs
	timer := raft.ResetTimer()
	ackCount := 0

	//send out vote request to all the replicas
	voteRequestMsg := util.VoteRequest{
		Term:         raft.Term,
		CandidateID:  raft.ServerID,
		LastLogIndex: 0,
		LastLogTerm:  0}

	raft.sendToServerReplica(util.Event{util.TypeVoteRequest, voteRequestMsg}, -1)

	for {
		event := <-raft.EventInCh
		switch event.Type {
		case util.TypeClientAppendRequest:
			// Do not handle clients in follower mode. Send it back up the
			// pipe with committed = false
			message := event.Data.(util.AppendEntryRequest)
			message.LogEntry = LogEntryObj{message.LogEntry.Lsn(), message.LogEntry.Data(), false}
			raft.CommitCh <- message.LogEntry

		case util.TypeVoteRequest:
			message := event.Data.(util.VoteRequest)
			resp, changeState := raft.validateVoteRequest(message)

			raft.sendToServerReplica(util.Event{util.TypeVoteReply, resp}, message.CandidateID)
			if changeState {
				raft.LeaderID = -1
				log.Println("Server ", raft.ServerID, " changing state to ", FOLLOWER)
				return FOLLOWER
			}

		/*
			if message.Term < raft.Term {

			} //TODO: saurabh - add response

			if message.Term > raft.Term {
				raft.Term = message.Term
			}

			if message.Term != raft.LastVotedTerm {
				timer.Stop()
				timer = raft.ResetTimer()
				 reply ok to event.msg.serverid
				raft.sendToServerReplica(util.Event{util.TypeVoteReply, util.VoteReply{message.Term, true}}, message.CandidateID)
				//TODO: Saurabh - remember term, leader id (either in log or in separate file)
				raft.LastVotedTerm = message.Term
			}
		*/

		case util.TypeVoteReply:
			message := event.Data.(util.VoteReply)
			if message.Term > raft.Term {
				log.Printf("got vote from future term (%d>%d); abandoning election\n", message.Term, raft.Term)
				raft.LeaderID = -1
				raft.LastVotedCandidateID = -1
				return FOLLOWER
			}

			if message.Term < raft.Term {
				log.Printf("got vote from past term (%d<%d); ignoring\n", message.Term, raft.Term)
				break
			}

			if message.Result {
				log.Printf("%d voted for me\n", message.ServerID)
				ackCount++
			}
			// "Once a candidate wins an election, it becomes leader."
			if (ackCount + 1) >= (len(raft.ClusterConfig.Servers)/2 + 1) {
				log.Println("Selected leaderID = ", raft.ServerID)
				raft.LeaderID = raft.ServerID
				raft.LastVotedCandidateID = -1
				ackCount = 0
				return LEADER
			}

		case util.TypeAppendEntryRequest:
			timer.Stop()
			timer = raft.ResetTimer()
			message := event.Data.(util.AppendEntryRequest)

			//TODO: change the code to incorporate Raft logic
			raft.sendToServerReplica(util.Event{util.TypeAppendEntryResponse, true}, message.LeaderID)

			/*

				if message.Term < raft.Term {
					continue
				}
				   reset heartbeat timer
				   upgrade to event.msg.term if necessary
				   if prev entries of my log and event.msg match
				      add to disk log
				      flush disk log
				      respond ok to event.msg.serverid
				   else
				      respond err.
			*/
		case util.TypeTimeout:
			log.Println("Election Timed out for server ", raft.ServerID, ". Incrementing term")
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			timer.Stop()
			timer = raft.ResetTimer()
			return CANDIDATE // new state back to loop()
		}

	}
	return raft.CurrentState
}

func (raft *Raft) Leader() string {

	//start timer // to become candidate if no append reqs
	timer := raft.ResetTimer()
	ackCount := 0
	var previousLogEntryForConsensus util.LogEntry

	for {
		event := <-raft.EventInCh
		switch event.Type {
		case util.TypeClientAppendRequest:

			message := event.Data.(util.ClientAppendRequest)
			logEntry, err := raft.Append(message.Data)
			if err != nil {
				//TODO: this case would never happen
				*message.ResponseCh <- "ERR_APPEND"
				continue
			}

			//put entry in the global map
			ResponseChannelStore.Lock()
			ResponseChannelStore.m[logEntry.Lsn()] = message.ResponseCh
			ResponseChannelStore.Unlock()

			previousLogEntryForConsensus = logEntry

			//now check for consensus
			appendRPCMessage := util.AppendEntryRequest{logEntry, raft.ServerID, raft.LeaderCommitIndex,
				raft.LogEntryBuffer[len(raft.LogEntryBuffer)-1].Lsn(), raft.Term, 0} //TODO: Saurabh - leader term set to 0

			raft.sendToServerReplica(util.Event{util.TypeAppendEntryRequest, appendRPCMessage}, -1)

		case util.TypeVoteRequest:

			message := event.Data.(util.VoteRequest)
			resp, changeState := raft.validateVoteRequest(message)

			raft.sendToServerReplica(util.Event{util.TypeVoteReply, resp}, message.CandidateID)
			if changeState {
				if raft.LeaderID != -1 {
					log.Printf("abandoning old leader=%d\n", raft.LeaderID)
				}
				log.Println("new leader unknown", raft.ServerID)
				raft.LeaderID = -1
				return FOLLOWER
			}

		case util.TypeAppendEntryRequest:

		case util.TypeAppendEntryResponse:
			// Do not handle clients in follower mode. Send it back up the
			// pipe with committed = false
			message := event.Data.(util.AppendEntryResponse)

			if message.Term == raft.Term {
				if message.Success {
					ackCount++

					if (ackCount + 1) == (len(raft.ClusterConfig.Servers)/2 + 1) {
						//TODO: commit the log entry - write to disk

						//Now send the entry to KV store
						raft.CommitCh <- previousLogEntryForConsensus
						//update the commit index
						raft.CommitIndex++
						raft.LeaderCommitIndex = raft.CommitIndex

						//reset ackCount
						ackCount = 0
					}

				}
			}

		case util.TypeTimeout:
			log.Println("Election timed out. State changing to ", CANDIDATE)
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			timer.Stop()
			timer = raft.ResetTimer()
			return CANDIDATE // new state back to loop()

		}

	}
	return raft.CurrentState

}

func (raft *Raft) InitServer() {
	//register for RPC
	gob.Register(LogEntryObj{})

	/*
		//now start listening on the input channel from the connection handler for new append requests
		var message util.Event
		ackCount := 0
		var previousLogEntryForConsensus util.LogEntry

		for {
			message = <-raft.MessageInCh
			leaderConfig := raft.ClusterConfig.Servers[0]
			for _, server := range raft.ClusterConfig.Servers {
				if server.Id == raft.LeaderID {
					leaderConfig = server
				}
			}

			if message.Type == util.TypeClientAppendRequest {
				event := message.Data.(util.ClientAppendRequest)

				if raft.CurrentState != LEADER {

					*event.ResponseCh <- "ERR_REDIRECT " + leaderConfig.Hostname + " " + strconv.Itoa(leaderConfig.ClientPort) + "\r\n"

				} else {

					logEntry, err := raft.Append(event.Data)
					if err != nil {
						//TODO: this case would never happen
						*event.ResponseCh <- "ERR_APPEND"
						continue
					}

					//put entry in the global map
					ResponseChannelStore.Lock()
					ResponseChannelStore.m[logEntry.Lsn()] = event.ResponseCh
					ResponseChannelStore.Unlock()

					previousLogEntryForConsensus = logEntry

					//now check for consensus
					appendRPCMessage := util.AppendEntryRequest{logEntry, raft.ServerID, raft.LeaderCommitIndex,
						raft.LogEntryBuffer[len(raft.LogEntryBuffer)-1].Lsn(), raft.Term, 0} //TODO: Saurabh - leader term set to 0

					raft.sendToServerReplica(util.Event{util.TypeAppendEntryRequest, appendRPCMessage}, -1)
				}
			} else if message.Type == util.TypeAppendEntryRequest {
				event := message.Data.(util.AppendEntryRequest)
				raft.sendToServerReplica(util.Event{util.TypeAppendEntryResponse, nil}, event.LeaderID)

			} else if message.Type == util.TypeAppendEntryResponse {
				ackCount++
				if ackCount == (len(raft.ClusterConfig.Servers) - 1) {
					//TODO: commit the log entry - write to disk

					//Now send the entry to KV store
					raft.CommitCh <- previousLogEntryForConsensus
					//update the commit index
					raft.CommitIndex++
					raft.LeaderCommitIndex = raft.CommitIndex

					//reset ackCount
					ackCount = 0
				}
			}
		}

	*/
}

func (raft *Raft) sendToServerReplica(message util.Event, replicaID int) {
	//From list of channels, find out the channel for #replicaID
	//and send out a message
	if replicaID == util.Broadcast {
		for serverID := range *raft.SharedEventChannelMap {
			if serverID == raft.ServerID {
				continue
			} else {
				outCh := (*raft.SharedEventChannelMap)[serverID]
				*outCh <- message
			}

		}
	} else {
		outCh := (*raft.SharedEventChannelMap)[replicaID]
		if outCh != nil {
			*outCh <- message
		} else {
			//TODO: Log a debug message showing error
		}

	}
}

// give vote
func (raft *Raft) validateVoteRequest(req util.VoteRequest) (util.VoteReply, bool) {
	if req.Term < raft.Term {
		return util.VoteReply{raft.Term, false, raft.ServerID}, false
	}

	changeState := false
	if req.Term > raft.Term {
		log.Printf("Vote Request from newer term (%d):  server %d", req.Term, raft.ServerID)
		raft.Term = req.Term
		raft.LastVotedCandidateID = -1
		raft.LeaderID = -1
		changeState = true
	}

	if (raft.CurrentState == LEADER && !changeState) ||
		(raft.LastVotedCandidateID != -1 && raft.LastVotedCandidateID != req.CandidateID) ||
		(raft.CurrentLsn > req.LastLogIndex || raft.LastVotedTerm > req.LastLogTerm) {
		return util.VoteReply{raft.Term, false, raft.ServerID}, changeState
	} else {
		raft.LastVotedCandidateID = req.CandidateID
		raft.ResetTimer()
		return util.VoteReply{raft.Term, true, raft.ServerID}, changeState
	}

}
