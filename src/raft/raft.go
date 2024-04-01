package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

const ElectionTimeoutLowerBoundMs = 400
const ElectionTimeoutHigherBoundMs = 500
const AppendEntriesIntervalMs = 100

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 3A Leader Election
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile server state
	commitIndex int

	// Volatile leader state
	nextIndex []int

	lastReceive time.Time
	serverState ServerState
	voteCount   int
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.serverState == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	// 3A Leader Election
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	// 3A Leader Election
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// currentTerm, for candidate to update itself
	reply.Term = rf.currentTerm

	// Update own term if outdated
	if args.Term > rf.currentTerm {
		DPrintf("server %d increments its term from %d to %d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	DPrintf("server %d received RequestVote (%+v) in term %d\n", rf.me, args, rf.currentTerm)

	// 2a) votedFor is null or candidateId  // TODO: Maybe something wrong here. Also: currentTerm is never really updated right? RequestVote überdenken!
	// if rf.votedFor != args.CandidateId {
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("server %d already voted for server %d\n", rf.me, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	// 2b) and candidate’s log is at least as up-to-date as receiver’s log, grant vote

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.

	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		fmt.Println("log of candidate not up to date")
		return
	}

	// Either candidate has higher term, or same term but at least the same log length
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	DPrintf("server %d voting for server %d\n", rf.me, args.CandidateId)
}

func (rf *Raft) isLogUpToDate(cLastIndex int, cLastTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastIndex(), rf.getLastTerm()

	if cLastTerm == myLastTerm {
		return cLastIndex >= myLastIndex
	}

	return cLastTerm > myLastTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update own term if outdated
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
	}

	DPrintf("server %d received AppendEntries (%+v) in term %d\n", rf.me, args, rf.currentTerm)

	lastIndex := rf.getLastIndex()
	rf.lastReceive = time.Now()

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > lastIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	startingIndex := args.PrevLogIndex + 1

	i, j := startingIndex, 0
	for ; i < lastIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}

	// 		Truncate beginning from the conflict
	rf.log = rf.log[:i]

	// 4. Append any new entries not already in the log
	args.Entries = args.Entries[j:]
	rf.log = append(rf.log, args.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// update commit index to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}

	reply.Success = true
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.serverState != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
				LeaderCommit: rf.commitIndex,
				Entries:      rf.log[rf.nextIndex[server]:],
			}
			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
	} else {
		rf.nextIndex[server] -= 1
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) LeaderElection() {
	for {
		state := rf.serverState

		if state != Follower {
			return
		}
		electionTimeout := rand.Intn(ElectionTimeoutHigherBoundMs-ElectionTimeoutLowerBoundMs) + ElectionTimeoutLowerBoundMs
		startTime := time.Now()

		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		rf.mu.Lock()

		if rf.lastReceive.Before(startTime) {
			if rf.serverState != Leader {
				go rf.KickoffElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) KickoffElection() {
	rf.mu.Lock()

	rf.ConvertToCandidate()
	electionTerm := rf.currentTerm

	DPrintf("server %d received no heartbeats and starts off leader election in new term %d\n", rf.me, electionTerm)

	args := &RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			DPrintf("server %d sending request vote %+v to server %d\n", rf.me, args, server)

			if ok := rf.sendRequestVote(server, args, reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				DPrintf("server %d respects higher term %d", rf.me, reply.Term)
				rf.ConvertToFollower(reply.Term)
				return
			}

			if !reply.VoteGranted {
				return
			}

			// Incoming votes for outdated requests
			if rf.serverState != Candidate || rf.currentTerm != electionTerm {
				return
			}

			rf.voteCount++
			DPrintf("%d/%d votes - server %d got vote from server %d in term %d\n", rf.voteCount, len(rf.peers), rf.me, server, rf.currentTerm)
			if rf.voteCount == len(rf.peers)/2+1 {
				rf.ConvertToLeader()
			}

		}(server)
	}
}

func (rf *Raft) ConvertToCandidate() {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.lastReceive = time.Now()
}

func (rf *Raft) ConvertToLeader() {
	DPrintf("----- new leader: server %d in term %d \n", rf.me, rf.currentTerm)
	rf.serverState = Leader
	rf.lastReceive = time.Now()
	rf.broadcastAppendEntries()
}

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.serverState = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastReceive = time.Now()
}

func (rf *Raft) Heartbeat() {
	for {
		time.Sleep(time.Duration(AppendEntriesIntervalMs) * time.Millisecond)
		rf.mu.Lock()
		if rf.serverState != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.broadcastAppendEntries()
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.serverState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.log = []LogEntry{
		{Term: 0},
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// Run Leader Election Goroutine
	go rf.LeaderElection()

	// Run Heartbeat Goroutine
	go rf.Heartbeat()

	return rf
}
