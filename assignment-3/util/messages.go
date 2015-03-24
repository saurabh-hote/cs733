package util

//Constants for request type
const (
	TypeAppendEntryRequest = iota
	TypeAppendEntryResponse
	TypeHeartBeat
	TypeVoteRequest
	TypeVoteReply
	TypeTimeout
	TypeClientAppendRequest
)

const (
	Broadcast = -1
)


type Lsn uint64      //Log sequence number, unique for all time.
type ErrRedirect int // See Log.Append. Implements Error interface.
type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}

type Event struct {
	Type int
	Data interface{}
}

//This struct will be sent as RPC message between the replicas
type AppendEntryRequest struct {
	LogEntry               LogEntry
	LeaderID               int
	LeaderCommitIndex      Lsn
	PreviousSharedLogIndex Lsn
	Term                   uint64
	PreviousSharedLogTerm  uint64
}

type AppendEntryResponse struct {
	//Reply strucrure.
	Term             uint64
	Success          bool
	PreviousLogIndex int64
	ExpectedIndex    int64
}

type HeartBeat struct {
	LeaderID          int
	PreviousLogIndex  int64
	PreviousLogTerm   uint64
	LeaderCommitIndex int64
	Term              uint64
}

type VoteRequest struct {
	Term         uint64
	CandidateID  int
	LastLogIndex Lsn
	LastLogTerm  uint64
}

type VoteReply struct {
	Term   uint64
	Result bool
	ServerID int
}

type Timeout struct{}

type ClientAppendRequest struct {
	Data       []byte
	ResponseCh *chan string
}
