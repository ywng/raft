package main

import (
	"bytes"
	"encoding/gob"
	"log"
	rand "math/rand"
	"sync"
	"time"

	context "golang.org/x/net/context"

	"github.com/raft/pb"
)

const (
	follower  = 1
	candidate = 2
	leader    = 3
	// for cluster membership change
	//shutdown  = 4

	//different timeout in ms
	ELECTION_TIMEOUT_LOWER_BOUND = 1000
	ELECTION_TIMEOUT_UPPER_BOUND = 4000
	HEARTBEAT_TIMEOUT            = 500
	LOG_COMPACTION_LIMIT         = 30 //-1 means no log compaction
)

type AppendResponse struct {
	ret         *pb.AppendEntriesRet
	err         error
	peer        string
	matchIndex  int64
	requestTerm int64
}

type VoteResponse struct {
	ret         *pb.RequestVoteRet
	err         error
	peer        string
	requestTerm int64
}

type InstallSnapshotResponse struct {
	ret         *pb.InstallSnapshotRet
	err         error
	peer        string
	requestTerm int64
}

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Messages that can be passed from the Raft RPC server to the main loop for InstallSnapshot
type InstallSnapshotInput struct {
	arg      *pb.InstallSnapshotArgs
	response chan pb.InstallSnapshotRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan          chan AppendEntriesInput
	VoteChan            chan VoteInput
	InstallSnapshotChan chan InstallSnapshotInput

	//lock to protect shared access to this raft server state
	//though in our lab exercise, this shouldn't be a concern
	//as only one main go routine is accessing the state at any time
	mu        sync.Mutex
	me        string
	leader    string
	persister *Persister // Object to hold the raft persisted states

	state      int64
	quorumSize int64

	//this raft server persistent states
	currentTerm  int64
	votedFor     string
	lastVoteTerm int64
	log          []*pb.Entry

	//this raft server volatile states
	commitIndex int64
	lastApplied int64

	//leader's volatile states
	nextIndex  map[string]int64
	matchIndex map[string]int64
	//map of logIndex -> client response ch
	clientsResponse map[int64]chan pb.Result

	//timer & ticker for election timeout and heartbeat
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	randSeed       *rand.Rand

	//peers
	peers *arrayPeers

	//for snapshot
	lastSnapshotLogEntry *pb.Entry

	//TO DO: for memershutdown
	//killServer chan int64
}

//to save persistent raft states
func (r *Raft) persist() {
	write := new(bytes.Buffer)
	encoder := gob.NewEncoder(write)
	encoder.Encode(r.currentTerm)
	encoder.Encode(r.votedFor)
	encoder.Encode(r.log)
	data := write.Bytes()
	r.persister.SaveRaftState(data)
}

func (r *Raft) leaderStatePrep() {
	r.state = leader
	r.leader = r.me
	// reset the heartbeat timer & stop election timer
	restartTimer(r.heartBeatTimer, HEARTBEAT_TIMEOUT*time.Millisecond)
	stopTimer(r.electionTimer)

	//initialise leader's volatile state
	r.nextIndex = make(map[string]int64)
	r.matchIndex = make(map[string]int64)
	r.clientsResponse = make(map[int64]chan pb.Result)

	index := r.getLastLogIndex() + 1
	for _, peer := range *r.peers {
		r.nextIndex[peer] = index
		//match index is a conservative measurement of what prefix of the log the leader shares with given followers
		//which we won't know beforehead, initialised to 0, essentially mean none of entries
		r.matchIndex[peer] = 0
	}
}

func (r *Raft) fallbackToFollower() {
	r.state = follower
	// reset the election timer & stop heartbeat timer
	restartTimer(r.electionTimer, randomDuration(r.randSeed))
	stopTimer(r.heartBeatTimer)
}

func (r *Raft) deleteEntryFrom(index int64) {
	firstIndex := r.log[0].Index
	if r.getLastLogIndex() < index {
		return
	} else {
		sliceIndex := index - firstIndex
		r.log = r.log[:sliceIndex]
	}
}

func (r *Raft) deleteAllEntries() {
	r.log = nil
	r.addLogEntry(r.lastSnapshotLogEntry)
}

func (r *Raft) getFirstLogIndex() int64 {
	return r.log[0].Index
}

func (r *Raft) getLastLogIndex() int64 {
	return r.log[len(r.log)-1].Index
}

func (r *Raft) getLastLogTerm() int64 {
	return r.log[len(r.log)-1].Term
}

func (r *Raft) getLogLen() int64 {
	return int64(len(r.log))
}

func (r *Raft) addLogEntry(entry *pb.Entry) {
	r.log = append(r.log, entry)
}

// the logic of index-firstIndex is for snapshot logic
// after snapshot, the entry.Index is not necessarily the index of the log array
func (r *Raft) getLogEntry(index int64) (*pb.Entry, bool) {
	var entry *pb.Entry
	if r.getLogLen() == 0 {
		return entry, false
	}
	firstIndex := r.log[0].Index
	if r.getLastLogIndex() < index || firstIndex > index {
		return entry, false
	} else {
		return r.log[index-firstIndex], true
	}
}

func (r *Raft) getEntryFrom(index int64) []*pb.Entry {
	firstIndex := r.log[0].Index
	sliceIndex := index - firstIndex
	return r.log[sliceIndex:]
}

func (r *Raft) Compaction(index int64) {
	log.Printf("Doing compaction, up to index: %d, first entry index: %d.", index, r.getFirstLogIndex())
	if r.lastSnapshotLogEntry == nil || index > r.lastSnapshotLogEntry.Index && index > r.getFirstLogIndex() {
		r.lastSnapshotLogEntry, _ = r.getLogEntry(index)
		r.log = r.getEntryFrom(index)
		r.persist()
	}
}

// this check the raft server's log if any committed but unhandled commands
// after the command is handled, it will response to the client by HandleCommand function
func (r *Raft) ProcessLogs(s *KVStore) {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry, _ := r.getLogEntry(r.lastApplied)

		//only leader reply to client's request
		//if not leader, just output to a dummy channel / nil channel
		var responseChan chan pb.Result
		if r.state == leader {
			responseChan = r.clientsResponse[entry.Index]
		} else {
			responseChan = nil
		}
		op := InputChannelType{command: *entry.Cmd, response: responseChan}
		s.HandleCommand(op)

		delete(r.clientsResponse, entry.Index)
		log.Printf("Applied committed log to the state machine. Index: %d, Command: %s.", entry.Index, entry.Cmd.Operation)
	}

	log.Printf("Length of log: %v", len(r.log))
	//stop the election timeout timer during compaction which might take longer time than the timeout limit
	//r.electionTimer.Stop()
	//check if we reach compaction limit, and do compaction
	if LOG_COMPACTION_LIMIT != -1 && len(r.log) >= LOG_COMPACTION_LIMIT {
		write := new(bytes.Buffer)
		encoder := gob.NewEncoder(write)
		encoder.Encode(s.store)
		data := write.Bytes()
		r.persister.SaveSnapshot(data)
		log.Printf("Server starts compaction, compact up to index: %v, length of log: %v", r.lastApplied, len(r.log))
		r.Compaction(r.lastApplied)
	}
	//resume the election timer after compaction
	//restartTimer(r.electionTimer, randomDuration(r.randSeed))
}

// this is used to construct and send a vote request to all peers
func (r *Raft) sendVoteRequests(peerClients map[string]pb.RaftClient, voteResponseChan chan VoteResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = candidate
	r.currentTerm++
	r.votedFor = r.me
	//clear out the previous term leader, this term leader is not yet known
	r.leader = ""
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := int64(0)
	if lastLogIndex != 0 {
		lastLogTerm = r.getLastLogTerm()
	}

	r.persist()

	for p, c := range peerClients {
		// Send in parallel so we don't wait for each client.
		log.Printf("Send vote request to %s, currentTerm: %d, lastLogIndex: %d, lastLogTerm: %d",
			p, r.currentTerm, lastLogIndex, lastLogTerm)
		go func(c pb.RaftClient, p string) {
			ret, err := c.RequestVote(context.Background(),
				&pb.RequestVoteArgs{Term: r.currentTerm,
					CandidateID:  r.me,
					LastLogIndex: lastLogIndex,
					LasLogTerm:   lastLogTerm})
			voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p, requestTerm: r.currentTerm}
		}(c, p)
	}
}

// this is used to construct and send an append entry request to all peers
func (r *Raft) sendApeendEntries(peerClients map[string]pb.RaftClient, appendResponseChan chan AppendResponse, snapshotResponseChan chan InstallSnapshotResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for p, c := range peerClients {
		r.sendApeendEntriesTo(p, c, appendResponseChan, snapshotResponseChan)
	}
}

// this is used to construct and send an append entry request to given peer (var p)
func (r *Raft) sendApeendEntriesTo(p string, c pb.RaftClient, appendResponseChan chan AppendResponse, snapshotResponseChan chan InstallSnapshotResponse) {
	var isHeartBeat bool
	if r.getLastLogIndex() >= r.nextIndex[p] {
		isHeartBeat = false
	} else {
		isHeartBeat = true
	}

	prevLogTerm := int64(0)
	prevLogIndex := r.nextIndex[p] - 1

	if prevLogIndex != 0 {
		entry, ok := r.getLogEntry(prevLogIndex)
		if ok {
			prevLogTerm = entry.Term
		} else {
			//cannot get the  prevLogIndex,
			//it is snapshot... sned install snapshot to peer
			installSnapshotArgs := &pb.InstallSnapshotArgs{
				Term:         r.currentTerm,
				LeaderID:     r.me,
				LastLogEntry: r.lastSnapshotLogEntry,
				Data:         r.persister.ReadSnapshot()}
			log.Printf("Sent InstallSnapshot request to %s, senderCurrentTerm: %d, prevLogIndex: %d, prevLogTerm: %d, commitIndex: %d, lastSnapshotLogIndex: %d, snapshotSize: %d.",
				p, r.currentTerm, prevLogIndex, prevLogTerm, r.commitIndex, r.lastSnapshotLogEntry.Index, r.persister.SnapshotSize())
			go func(c pb.RaftClient, p string) {
				ret, err := c.InstallSnapshot(context.Background(), installSnapshotArgs)
				snapshotResponseChan <- InstallSnapshotResponse{ret: ret, err: err, peer: p, requestTerm: r.currentTerm}
			}(c, p)

			return
		}
	}

	var args *pb.AppendEntriesArgs
	if isHeartBeat {
		args = &pb.AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: r.commitIndex,
			Entries:      nil}
	} else {
		if _, ok := r.getLogEntry(prevLogIndex + 1); !ok {
			//cannot get the  prevLogIndex,
			//it is snapshot... sned install snapshot to peer

		}
		entries := r.getEntryFrom(prevLogIndex + 1)
		args = &pb.AppendEntriesArgs{Term: r.currentTerm,
			LeaderID:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: r.commitIndex,
			Entries:      entries}
	}

	// Send in parallel so we don't wait for each client.
	log.Printf("Sent append entry request to %s, senderCurrentTerm: %d, prevLogIndex: %d, prevLogTerm: %d, commitIndex: %d, entriesLen: %d.",
		p, r.currentTerm, prevLogIndex, prevLogTerm, r.commitIndex, int64(len(args.Entries)))
	go func(c pb.RaftClient, p string) {
		ret, err := c.AppendEntries(context.Background(), args)
		appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p,
			matchIndex: args.PrevLogIndex + int64(len(args.Entries)), requestTerm: r.currentTerm}
	}(c, p)
}

// put an append entry request to the given raft server's (var r) Append Entry Channel
// this is used/called to make an append entry request to given peer
func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// put a vote request to the given raft server's (var r) Vote Request Channel
// this is used/called to make a vote request to given peer
func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// put an install snapshot request to the given raft server's (var r) Install Snapshot Channel
// this is used/called to make a vote request to given peer
func (r *Raft) InstallSnapshot(ctx context.Context, arg *pb.InstallSnapshotArgs) (*pb.InstallSnapshotRet, error) {
	c := make(chan pb.InstallSnapshotRet)
	r.InstallSnapshotChan <- InstallSnapshotInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}
