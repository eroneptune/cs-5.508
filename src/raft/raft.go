package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	state     int32 // candidate/leader/follower
	voteCount int

	leaderTimer   *time.Timer
	electionTimer *time.Timer

	// persistent state
	currentTerm int64
	voteFor     int        // CandidateId that received vote in current Term (or null if none)
	log         []struct{} // TODO

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm nd whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	state := rf.state
	rf.mu.Unlock()
	// Your code here (2A).
	return int(term), state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// reject
	} else if rf.voteFor == -1 {
		// If votedFor is null, grant vote
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	} else {
		// or CandidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
	}

	DPrintf("===%d requestVote to server %d: %v %v", rf.me, args.CandidateId, args, reply)
}

func (rf *Raft) toState(newState int32) {
	// no Leader->Leader transition
	DPrintf("[%d] transfer %d to %d", rf.me, rf.state, newState)
	rf.state = newState

	switch newState {
	case Follower:
		rf.voteFor = -1
	case Candidate:
		rf.voteFor = rf.me
		rf.voteCount = 1
		go rf.leaderElection()
	case Leader:
		rf.voteFor = -1
		// reset
		rf.leaderTimer.Reset(time.Microsecond)
	}
}

func (rf *Raft) resetElectionTimer() {
	// pause for a random amount of time between 200 and 400 milliseconds.
	ms := 330 + (rand.Int63() % 220)
	rf.electionTimer.Reset(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) startElectionTimer() {
	for rf.killed() == false {
		rf.resetElectionTimer()

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()

			if rf.state != Leader {
				rf.toState(Candidate)
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()

	rf.currentTerm++
	t := rf.currentTerm
	me := rf.me

	req := &RequestVoteArgs{
		Term:        t,
		CandidateId: me,
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if me == i {
			continue
		}
		go rf.sendRequestVote(i, req, &RequestVoteReply{})
	}
}

func (rf *Raft) startHeartBeat() {

	for rf.killed() == false {
		select {
		case <-rf.leaderTimer.C:
			rf.mu.Lock()

			if rf.state == Leader {
				rf.sendHeartBeat()
			}
			// leader send heartbeat RPCs no more than ten times per second.
			rf.leaderTimer.Reset(110 * time.Millisecond)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	me := rf.me
	req := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: me,
	}

	for i := range rf.peers {
		if me == i {
			continue
		}
		go rf.sendAppendEntries(i, req, &AppendEntriesReply{})
	}
}

func (rf *Raft) appendEntriesToFollower() {

}

// check whether newTerm > currentTerm
// if so, convert fo follower
// return true if newTerm > currentTerm
func (rf *Raft) checkTerm(newTerm int64) bool {
	cterm := rf.currentTerm
	if newTerm > cterm {
		rf.currentTerm = newTerm
		rf.toState(Follower)
		return true
	}
	return false
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

	rf.mu.Lock()
	newTerm := rf.checkTerm(reply.Term)
	rf.mu.Unlock()
	if newTerm {
		return ok
	}

	if reply.VoteGranted {
		rf.mu.Lock()
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			if rf.state == Candidate {
				rf.toState(Leader)
			}
		}
		rf.mu.Unlock()
	}

	return ok
}

type AppendEntriesArgs struct {
	Term     int64
	LeaderId int
	// PrevLogIndex int
	// PrevLogTern  int
	// Entries      []struct{} // TODO
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	{
		// heatbeat RPC
	}
	if rf.currentTerm > args.Term {
		// Reply false if term < currentTerm
		return
	}

	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		rf.toState(Follower)
	}

	rf.resetElectionTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// rf.electionTimer = time.NewTimer(time.Second)
	rf.currentTerm = 0
	rf.electionTimer = time.NewTimer(time.Hour)
	rf.leaderTimer = time.NewTimer(time.Hour)
	go rf.startElectionTimer()
	go rf.startHeartBeat()
	rf.toState(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
