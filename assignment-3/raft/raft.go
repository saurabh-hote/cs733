package raft

import (
	"encoding/gob"
	util "github.com/saurabh-hote/cs733/assignment-3/util"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

//Constansts indicating the replica's state
const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

const (
	minTimeout           = 3 //in seconds
	maxTimeout           = 6
	heartbeatMsgInterval = 1
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
	ClusterConfig *ClusterConfig
	ServerID      int
	CommitCh      chan util.LogEntry
	LeaderID      int
	CurrentState  string //state of the server

	//entries for implementing Shared Log
	LogObj               *util.Log
	CurrentLsn           util.Lsn
	EventInCh            chan util.Event
	ServerQuitCh         chan chan struct{}
	ElectionTimer        <-chan time.Time
	Term                 uint64
	LastVotedTerm        uint64
	LastVotedCandidateID int
	ReplicaChannels      map[int]*gob.Encoder //map of replica id to socket

	timer *time.Timer
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
	raft.CurrentLsn = 0
	raft.LastVotedCandidateID = -1
	raft.LastVotedTerm = 0
	raft.ReplicaChannels = make(map[int]*gob.Encoder)

	raft.LogObj = util.NewLog(raft.ServerID) //State the file name
	raft.LogObj.SendToStateMachine = func(entry *util.LogEntryObj) {
		raft.CommitCh <- *entry
	}
	raft.LogObj.FirstRead()

	return raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

func (raft *Raft) Append(data []byte) (util.LogEntry, error) {
	if len(data) > 0 {
		raft.CurrentLsn++
		var logEntry util.LogEntryObj
		logEntry = util.LogEntryObj{raft.CurrentLsn, data, false, raft.Term}
		raft.LogObj.AppendEntry(logEntry)
		return logEntry, nil
	} else {
		return nil, new(ErrRedirect)
	}
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

func (raft *Raft) Follower() string {
	//start timer // to become candidate if no append reqs
	raft.timer = time.NewTimer(getRandomWaitDuration())

	for {
		select {
		case <-raft.timer.C:
			log.Println("Election timed out. State for server ", raft.ServerID, " changing to ", CANDIDATE)
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			raft.timer.Stop()
			raft.timer.Reset(getRandomWaitDuration())
			return CANDIDATE // new state back to loop()

		case event := <-raft.EventInCh:
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
					responseMsg = "ERR_REDIRECT NA\r\n"
				} else {
					responseMsg = "ERR_REDIRECT " + leaderConfig.Hostname + " " + strconv.Itoa(leaderConfig.ClientPort) + "\r\n"

				}
				(*message.ResponseCh) <- responseMsg

			case util.TypeVoteRequest:
				message := event.Data.(util.VoteRequest)

				voteResp, _ := raft.validateVoteRequest(message)
				raft.sendToServerReplica(util.Event{util.TypeVoteReply, voteResp}, message.CandidateID)
				raft.LeaderID = message.CandidateID
				raft.LastVotedCandidateID = message.CandidateID
				raft.LastVotedTerm = message.Term
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
				raft.timer.Stop()
				raft.timer.Reset(getRandomWaitDuration())
				message := event.Data.(util.AppendEntryRequest)
				log.Printf("At Server %d, received AppendEntryResquest from %d", raft.ServerID, message.LeaderID)

				if raft.LeaderID == -1 {
					raft.LeaderID = message.LeaderID
					log.Printf("Server %d found a new leader %d", raft.ServerID, message.LeaderID)
				}

				resp, changeOfLeader := raft.validateAppendEntryRequest(message)
				resp.ServerID = raft.ServerID

				log.Printf("At Server %d, sending AppendEntryResponse to leader %d", raft.ServerID, message.LeaderID)

				raft.sendToServerReplica(util.Event{util.TypeAppendEntryResponse, resp}, message.LeaderID)
				if changeOfLeader {
					raft.LeaderID = message.LeaderID
					return FOLLOWER
				}

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

			case util.TypeHeartBeat:
				//Make a check if the heartbeat is received from last voted server
				message := event.Data.(util.HeartBeat)
				log.Println("Heartbeat message receievd at server ", raft.ServerID, " from leader ", message.LeaderID)

				//TODO: Add the code for the follwing condition
				/*if message.LeaderID == raft.Leader() {
				}*/
				//				raft.timer.Stop()
				raft.timer.Reset(getRandomWaitDuration())
				return FOLLOWER
			}
		}
	}
	return raft.CurrentState
}

func (raft *Raft) Candidate() string {
	//start timer // to become candidate if no append reqs
	raft.timer = time.NewTimer(getRandomWaitDuration())
	ackCount := 0

	//send out vote request to all the replicas
	voteRequestMsg := util.VoteRequest{
		Term:         raft.Term,
		CandidateID:  raft.ServerID,
		LastLogIndex: raft.LogObj.LastIndex(),
		LastLogTerm:  raft.LogObj.LastTerm(),
	}

	raft.sendToServerReplica(util.Event{util.TypeVoteRequest, voteRequestMsg}, util.Broadcast)
	raft.LastVotedTerm = raft.Term

	for {
		select {
		case event := <-raft.EventInCh:
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
					responseMsg = "ERR_REDIRECT " + "NA"
				} else {
					responseMsg = "ERR_REDIRECT " + leaderConfig.Hostname + " " + strconv.Itoa(leaderConfig.ClientPort) + "\r\n"

				}
				(*message.ResponseCh) <- responseMsg

			case util.TypeVoteRequest:
				message := event.Data.(util.VoteRequest)
				resp, changeState := raft.validateVoteRequest(message)

				raft.sendToServerReplica(util.Event{util.TypeVoteReply, resp}, message.CandidateID)
				if changeState {
					raft.LeaderID = -1 //TODO: changed from  -1 to message.CandidateID
					raft.LastVotedCandidateID = message.CandidateID
					raft.LastVotedTerm = message.Term
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
					log.Printf("At server %d got vote from future term (%d>%d); abandoning election\n", raft.ServerID, message.Term, raft.Term)
					raft.LeaderID = -1
					raft.LastVotedCandidateID = -1
					return FOLLOWER
				}

				if message.Term < raft.Term {
					log.Printf("Server %d got vote from past term (%d<%d); ignoring\n", raft.ServerID, message.Term, raft.Term)
					break
				}

				if message.Result {
					log.Printf("At server %d, received vote from %d\n", raft.ServerID, message.ServerID)
					ackCount++
				}
				// "Once a candidate wins an election, it becomes leader."
				if (ackCount + 1) >= (len(raft.ClusterConfig.Servers)/2 + 1) {
					log.Println("At server ", raft.ServerID, " Selected leaderID = ", raft.ServerID)
					raft.LeaderID = raft.ServerID
					raft.LastVotedCandidateID = -1
					ackCount = 0
					return LEADER
				}

			case util.TypeAppendEntryRequest:
				raft.timer.Stop()
				raft.timer.Reset(getRandomWaitDuration())
				message := event.Data.(util.AppendEntryRequest)
				resp, stepDown := raft.validateAppendEntryRequest(message)
				resp.ServerID = raft.ServerID

				raft.sendToServerReplica(util.Event{util.TypeAppendEntryResponse, resp}, message.LeaderID)

				if stepDown {
					raft.LeaderID = message.LeaderID
					return FOLLOWER
				}

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

			case util.TypeHeartBeat:
				//Make a check if the heartbeat is received from last voted server
				message := event.Data.(util.HeartBeat)
				log.Println("Heartbeat message receievd at server ", raft.ServerID, " from leader ", message.LeaderID)

				//TODO: Add the code for the follwing condition
				/*if message.LeaderID == raft.Leader() {
				}*/
				if message.Term > raft.Term {
					//					raft.timer.Stop()
					raft.timer.Reset(getRandomWaitDuration())
					return FOLLOWER
				} else {
					raft.timer.Reset(getRandomWaitDuration())
					return CANDIDATE
				}
			}

		case <-raft.timer.C:
			log.Println("Election Timed out for server ", raft.ServerID, ". Incrementing term")
			raft.Term++
			raft.LastVotedCandidateID = -1
			raft.LeaderID = -1
			raft.timer.Stop()
			raft.timer.Reset(getRandomWaitDuration())
			return CANDIDATE // new state back to loop()

		}
	}
	return raft.CurrentState
}

func (raft *Raft) Leader() string {
	//TODO: remove this code

	//Start hearbeat sending routine
	go raft.sendHeartbeat()

	//start timer // to become candidate if no append reqs
	raft.timer = time.NewTimer(getRandomWaitDuration())
	ackCount := 0
	nakCount := 0
	var previousLogEntryForConsensus util.LogEntry
	quorumsReceived := 0

	for {
		select {
		case event := <-raft.EventInCh:
			switch event.Type {
			case util.TypeClientAppendRequest:
				log.Printf("At Server %d, received client append request", raft.ServerID)
				appendRPCMessage := util.AppendEntryRequest{
					LeaderID:               raft.ServerID,
					LeaderCommitIndex:      raft.LogObj.GetCommitIndex(),
					PreviousSharedLogIndex: raft.LogObj.LastIndex(),
					Term: raft.Term,
					PreviousSharedLogTerm: raft.LogObj.LastTerm(),
				}

				message := event.Data.(util.ClientAppendRequest)
				logEntry, err := raft.Append(message.Data)
				if err != nil {
					//TODO: this case would never happen
					*message.ResponseCh <- "ERR_APPEND"
					continue
				}

				//put entry in the global map
				util.ResponseChannelStore.Lock()
				util.ResponseChannelStore.M[logEntry.Lsn()] = message.ResponseCh
				util.ResponseChannelStore.Unlock()

				previousLogEntryForConsensus = logEntry

				//now check for consensus
				appendRPCMessage.LogEntry = logEntry

				log.Printf("At Server %d, sending TypeAppendEntryRequest", raft.ServerID)

				raft.sendToServerReplica(util.Event{util.TypeAppendEntryRequest, appendRPCMessage}, util.Broadcast)

			case util.TypeVoteRequest:

				message := event.Data.(util.VoteRequest)
				resp, changeState := raft.validateVoteRequest(message)

				raft.sendToServerReplica(util.Event{util.TypeVoteReply, resp}, message.CandidateID)
				if changeState {
					log.Printf("At Server %d, change of leader from %d to %d\n", raft.ServerID, raft.LeaderID, message.CandidateID)
					raft.LeaderID = message.CandidateID //TODO: change from -1 to
					return FOLLOWER
				}

			case util.TypeAppendEntryRequest:
				message := event.Data.(util.AppendEntryRequest)
				log.Println("Error: Two servers in leader state found. ", "IDs - ", raft.ServerID, ":", message.LeaderID, " Terms - ", raft.Term, ":", message.Term)
				resp, changeState := raft.validateAppendEntryRequest(message)

				//send serponse to the leader
				raft.sendToServerReplica(util.Event{util.TypeAppendEntryResponse, resp}, message.LeaderID)

				if changeState {
					raft.LeaderID = message.LeaderID
					return FOLLOWER
				}

			case util.TypeAppendEntryResponse:
				// Do not handle clients in follower mode. Send it back up the
				// pipe with committed = false
				message := event.Data.(util.AppendEntryResponse)
				log.Printf("At Server %d, received AppendEntryResponse", raft.ServerID)

				if message.Term == raft.Term {
					if message.Success {
						ackCount++

						if (ackCount + 1) == (len(raft.ClusterConfig.Servers)/2 + 1) {
							//TODO: commit the log entry - write to disk

							//Now send the entry to KV store
							raft.CommitCh <- previousLogEntryForConsensus

							quorumsReceived++

							//reset ackCount
							ackCount = 0
							nakCount = 0
						}

					} else {
						log.Printf("At Server %d, received negative response from server %d", raft.ServerID, message.ServerID)
						nakCount++

						if (nakCount + 1) == (len(raft.ClusterConfig.Servers)/2 + 1) {

							util.ResponseChannelStore.RLock()
							responseChannel := util.ResponseChannelStore.M[previousLogEntryForConsensus.Lsn()]
							util.ResponseChannelStore.RUnlock()

							if responseChannel == nil {
								log.Printf("At server %d, Response channel for LogEntry with lsn %d not found", raft.ServerID, int(previousLogEntryForConsensus.Lsn()))
							} else {
								//Delete the entry for response channel handle
								util.ResponseChannelStore.Lock()
								delete(util.ResponseChannelStore.M, previousLogEntryForConsensus.Lsn())
								util.ResponseChannelStore.Unlock()
								*responseChannel <- "ERR_QUORUM_NOT_ACHIEVED\r\n"
							}

							//reset ackCount and nakCount
							nakCount = 0
							ackCount = 0
						}

					}
				} else {
					log.Printf("At Server %d, received AppendEntryResponse for older term", raft.ServerID)
				}

			case util.TypeTimeout:

			}
		case <-raft.timer.C:
			/*
				log.Println("Election timed out at server.", raft.ServerID, " Changing state to ", CANDIDATE)
				raft.Term++
				raft.LastVotedCandidateID = -1
				raft.LeaderID = -1
				raft.timer.Stop()
				raft.timer.Reset(getRandomWaitDuration())
				return CANDIDATE // new state back to loop()
			*/
		}

		if quorumsReceived >= 2 {
			raft.LogObj.CommitTo(raft.LogObj.LastIndex() - 1)
			log.Printf("At Server %d, committing to %d", raft.ServerID, raft.LogObj.LastIndex()-1)
		}

	}
	return raft.CurrentState

}

func (raft *Raft) InitServer() {
	//register for RPC
	gob.Register(util.LogEntryObj{})
	gob.Register(util.Event{})
	gob.Register(util.VoteRequest{})
	gob.Register(util.VoteReply{})
	gob.Register(util.AppendEntryRequest{})
	gob.Register(util.AppendEntryResponse{})
	gob.Register(util.ClientAppendRequest{})
	gob.Register(util.Timeout{})
	gob.Register(util.HeartBeat{})

	go raft.startListeningForReplicaEvents()

	go raft.createReplicaConnections()

	raft.Loop()
}

// give vote
func (raft *Raft) validateVoteRequest(req util.VoteRequest) (util.VoteReply, bool) {
	if req.Term < raft.Term {
		return util.VoteReply{raft.Term, false, raft.ServerID}, false
	}

	changeState := false
	if req.Term > raft.Term {
		log.Printf("At Server %d, Vote Request with newer term (%d)", raft.ServerID, req.Term)
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
		raft.timer.Stop()
		raft.timer.Reset(getRandomWaitDuration())
		return util.VoteReply{raft.Term, true, raft.ServerID}, changeState
	}

}

// heartbeat from sevrer recieved
func (raft *Raft) validateAppendEntryRequest(req util.AppendEntryRequest) (util.AppendEntryResponse, bool) {

	if req.Term < raft.Term {
		return util.AppendEntryResponse{
			Term:    raft.Term,
			Success: false,
		}, false
	}
	stepDown := false

	if req.Term > raft.Term {
		raft.Term = req.Term
		raft.LastVotedCandidateID = -1
		stepDown = true
		if raft.CurrentState == LEADER {
			log.Printf("At Server %d, AppendEntryRequest with higher term %d received from leader %d", raft.ServerID, raft.Term, req.LeaderID)
		}
		log.Printf("At Server %d, new leader is %d", raft.ServerID, req.LeaderID)
	}

	if raft.CurrentState == CANDIDATE && req.LeaderID != raft.LeaderID && req.Term >= raft.Term {
		raft.Term = req.Term
		raft.LastVotedCandidateID = -1
		stepDown = true
	}

	raft.timer.Stop()
	raft.timer.Reset(getRandomWaitDuration())

	// Reject if log doesn't contain a matching previous entry
	log.Printf("At Server %d, Performing log discard check with index = %d", raft.ServerID, req.PreviousSharedLogIndex)

	err := raft.LogObj.Discard(req.PreviousSharedLogIndex, req.PreviousSharedLogTerm)

	if err != nil {
		log.Printf("At Server %d, Log discard check failed - %s", raft.ServerID, err)
		return util.AppendEntryResponse{
			Term:    raft.Term,
			Success: false,
		}, stepDown
	}

	entry := req.LogEntry
	// Append entry to the log
	if err := raft.LogObj.AppendEntry(entry.(util.LogEntryObj)); err != nil {
		log.Printf("At Server %d, Log Append failed - %s", raft.ServerID, err.Error())

		return util.AppendEntryResponse{
			Term:    raft.Term,
			Success: false,
		}, stepDown
	}

	if req.LeaderCommitIndex > 0 && req.LeaderCommitIndex > raft.LogObj.GetCommitIndex() {
		log.Printf("At Server %d, Committing to index %d", raft.ServerID, req.LeaderCommitIndex)
		if err := raft.LogObj.CommitTo(req.LeaderCommitIndex); err != nil {
			return util.AppendEntryResponse{
				Term:    raft.Term,
				Success: false,
			}, stepDown
		}
	}

	return util.AppendEntryResponse{
		Term:    raft.Term,
		Success: true,
	}, stepDown

}

func (raft *Raft) sendHeartbeat() {
	timer := time.NewTimer(time.Duration(heartbeatMsgInterval) * time.Second)

	for {
		if raft.CurrentState == LEADER {
			timer.Reset(time.Duration(heartbeatMsgInterval) * time.Second)
			heartbeatMsg := util.HeartBeat{
				LeaderID:          raft.ServerID,
				PreviousLogIndex:  raft.LogObj.LastIndex(),
				PreviousLogTerm:   raft.LogObj.LastTerm(),
				LeaderCommitIndex: raft.LogObj.GetCommitIndex(),
				Term:              raft.Term,
			}

			message := util.Event{util.TypeHeartBeat, heartbeatMsg}

			raft.sendToServerReplica(message, util.Broadcast)
			log.Println("Hearbeat sent by leader ", raft.ServerID)

			//Wait for hearbeat timeout
			<-timer.C
			//			/timer.Reset(time.Duration(heartbeatMsgInterval) * time.Second)
		}
	}
}

func getRandomWaitDuration() time.Duration {
	//Perfrom random selection for timeout peroid
	randomVal := rand.Intn(int(maxTimeout-minTimeout)) + (maxTimeout - minTimeout)
	return time.Duration(randomVal) * time.Second
}

func (raft *Raft) createReplicaConnections() {
	for _, server := range raft.ClusterConfig.Servers {
		if server.Id == raft.ServerID {
			continue
		} else {
			raft.connect(server.Id, server.Hostname, server.LogPort)
		}
	}
}

func (raft *Raft) connect(serverID int, hostname string, port int) {
	for {
		conn, err := net.Dial("tcp", hostname+":"+strconv.Itoa(port))
		if err != nil {
			//	log.Println("At server " + strconv.Itoa(raft.ServerID) + ", connect error " + err.Error())
		} else {
			encoder := gob.NewEncoder(conn)
			raft.ReplicaChannels[serverID] = encoder
			break
		}
	}
}

func (raft *Raft) startListeningForReplicaEvents() {
	serverConfig := raft.ClusterConfig.Servers[0]
	for _, server := range raft.ClusterConfig.Servers {
		if server.Id == raft.ServerID {
			serverConfig = server
		}
	}

	psock, err := net.Listen("tcp", ":"+strconv.Itoa(serverConfig.LogPort))
	if err != nil {
		return
	}
	for {
		conn, err := psock.Accept()
		if err != nil {
			return
		}
		go raft.RequestHandler(conn)
	}
}

func (raft *Raft) RequestHandler(conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	for {
		event := new(util.Event)
		err := decoder.Decode(&event)
		if err != nil {
			log.Println("Error Socket: " + err.Error())
			//TODO: handle
			break
		}
		raft.EventInCh <- *event
	}
}

func (raft *Raft) sendToServerReplica(message util.Event, replicaID int) {
	//From list of channels, find out the channel for #replicaID
	//and send out a message
	var err error
	if replicaID == util.Broadcast {

		for serverID, replicaSocket := range raft.ReplicaChannels {
			if serverID == raft.ServerID {
				continue
			} else {
				err = replicaSocket.Encode(&message)
				if err != nil {
					log.Printf("At server %d, Send error - %s", raft.ServerID, err.Error())
					log.Printf("At server %d, Attempting reconnect with server %d", raft.ServerID, serverID)

					serverConfig := raft.ClusterConfig.Servers[0]
					for _, server := range raft.ClusterConfig.Servers {
						if server.Id == serverID {
							serverConfig = server
						}
					}
					raft.connect(serverID, serverConfig.Hostname, serverConfig.LogPort)
				}
			}
		}
	} else {
		encoder := raft.ReplicaChannels[replicaID]
		err = encoder.Encode(message)
		if err != nil {
			log.Printf("At server %d, Send error - %s", raft.ServerID, err.Error())
			log.Printf("At server %d, Attempting reconnect with server %d", raft.ServerID, replicaID)

			serverConfig := raft.ClusterConfig.Servers[0]
			for _, server := range raft.ClusterConfig.Servers {
				if server.Id == replicaID {
					serverConfig = server
				}
			}

			raft.connect(replicaID, serverConfig.Hostname, serverConfig.LogPort)
		}
	}
}