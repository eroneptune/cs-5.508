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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

type Log struct {
	Term    int64
	Index   int
	Command interface{}
}

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

	state         int32 // candidate/leader/follower
	voteCount     int
	leaderTimer   *time.Timer
	electionTimer *time.Timer

	// persistent state
	currentTerm   int64
	voteFor       int // CandidateId that received vote in current Term (or null if none)
	log           []Log
	snapshotTerm  int64
	snapshotIndex int
	snapshot      []byte

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

// return currentTerm nd whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	state := rf.state
	rf.mu.Unlock()
	return int(term), state == Leader
}

// get the index of the last log
func (rf *Raft) GetLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// get the term of the last log
func (rf *Raft) GetLastLogTerm() int64 {
	return rf.log[len(rf.log)-1].Term
}

// get the log term by index
func (rf *Raft) GetLogTerm(index int) int64 {
	return rf.log[index-rf.snapshotIndex].Term
}

// get the log command by index
func (rf *Raft) GetLogCommand(index int) interface{} {
	return rf.log[index-rf.snapshotIndex].Command
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() { // TODO
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.snapshotIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)

	// rf.readPersist2(rf.persister.ReadRaftState())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) { // TODO
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int64
	var voteFor int
	var log []Log
	var snapshotTerm int64
	var snapshotIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotTerm) != nil ||
		d.Decode(&snapshotIndex) != nil {
		// error...
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.snapshotTerm = snapshotTerm
		rf.snapshotIndex = snapshotIndex
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		rf.snapshot = rf.persister.ReadSnapshot() // load the snapshot
		rf.mu.Unlock()
	}
}

type SnapshotArgs struct {
	Term              int64
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int64
	Data              []byte
}

type SnapshotResp struct {
	Term int64
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		// old snapshot
		return
	}

	temp := rf.snapshotIndex

	rf.snapshotTerm = rf.GetLogTerm(index)
	rf.snapshotIndex = index
	rf.log = rf.log[index-temp:]

	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.snapshot = snapshot

	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotResp) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotResp) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// Reply immediately if term < currentTerm
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.snapshotIndex {
		// old snapshot
		rf.mu.Unlock()
		return
	}

	if rf.GetLastLogIndex() > args.LastIncludedIndex {
		// discard the log before the snapshot
		rf.log = rf.log[args.LastIncludedIndex-rf.GetLastLogIndex():]
	} else {
		// discard the entire log
		rf.log = []Log{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	if args.LastIncludedIndex > rf.commitIndex {
		// update the commitIndex
		rf.commitIndex = args.LastIncludedIndex
	}

	if args.LastIncludedIndex > rf.lastApplied {
		// update the lastApplied
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()
	rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: args.LastIncludedIndex,
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int64
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

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// Reply immediately if term < currentTerm
		reply.Term = rf.currentTerm
		return
	}

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		// If votedFor is null or candidateId
		if args.LastLogTerm > rf.GetLastLogTerm() {
			// the candidate's log has the larger term
			reply.VoteGranted = true
			rf.resetElectionTimer()
			rf.voteFor = args.CandidateId
			rf.persist()
		} else if args.LastLogTerm == rf.GetLastLogTerm() && args.LastLogIndex >= rf.GetLastLogIndex() {
			// same term, candidate with larger index
			reply.VoteGranted = true
			rf.resetElectionTimer()
			rf.voteFor = args.CandidateId
			rf.persist()
		}
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
		rf.persist()
	case Candidate:
		rf.voteFor = rf.me
		rf.persist()
		rf.voteCount = 1
		go rf.leaderElection()
	case Leader:
		rf.initLeader()
		rf.leaderTimer.Reset(time.Microsecond)
	}
}

// reset the election timer
func (rf *Raft) resetElectionTimer() {
	// pause for a random amount of time between 700 and 1000 milliseconds.
	ms := 700 + (rand.Int63() % 300)
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

// start a leader election
func (rf *Raft) leaderElection() {
	rf.mu.Lock()

	rf.currentTerm++
	t := rf.currentTerm
	me := rf.me
	req := &RequestVoteArgs{
		Term:         t,
		CandidateId:  me,
		LastLogIndex: rf.GetLastLogIndex(),
		LastLogTerm:  rf.GetLastLogTerm(),
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if me == i {
			continue
		}
		go rf.sendRequestVote(i, req, &RequestVoteReply{})
	}
}

// start heart beat
func (rf *Raft) startHeartBeat() {

	for rf.killed() == false {
		select {
		case <-rf.leaderTimer.C:
			rf.mu.Lock()

			if rf.state == Leader {
				rf.sendHeartBeat()
			}
			rf.leaderTimer.Reset(120 * time.Millisecond)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[idx] <= rf.snapshotIndex {
					// nextIndex < snapshotIndex, then send the latest snapshot
					args := SnapshotArgs{
						rf.currentTerm,
						rf.me,
						rf.snapshotIndex,
						rf.snapshotTerm,
						rf.snapshot,
					}
					rf.mu.Unlock()
					reply := &SnapshotResp{}
					ret := rf.sendInstallSnapshot(idx, &args, reply)
					rf.mu.Lock()

					if !ret || rf.state != Leader || rf.currentTerm != args.Term {
						// rpc call fails or the state has changed or the term has changed
						rf.mu.Unlock()
						return
					}

					if rf.checkTerm(reply.Term) {
						// the reply term > currentTerm, become follower
						rf.mu.Unlock()
						return
					}

					rf.matchIndex[idx] = rf.snapshotIndex
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1

					rf.updateCommitIndex()
					rf.mu.Unlock()
				} else {
					// send append entries
					args := AppendEntriesArgs{
						rf.currentTerm,
						rf.me,
						rf.nextIndex[idx] - 1,
						rf.GetLogTerm(rf.nextIndex[idx] - 1),
						append(make([]Log, 0), rf.log[(rf.nextIndex[idx]-rf.snapshotIndex):]...),
						rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ret := rf.sendAppendEntries(idx, &args, reply)
					rf.mu.Lock()

					if !ret || rf.state != Leader || rf.currentTerm != args.Term {
						// rpc call fails or the state has changed or the term has changed
						rf.mu.Unlock()
						return
					}
					if rf.checkTerm(reply.Term) {
						rf.mu.Unlock()
						return
					}

					if !reply.Success { // TODO
						if reply.Xlen != 0 {
							// follower's log is too short:
							rf.nextIndex[idx] = reply.Xlen
						} else {
							tarIndex := reply.XIndex
							if reply.XTerm != 0 {
								logSize := rf.GetLastLogIndex() + 1
								for i := rf.snapshotIndex; i < logSize; i++ {
									if rf.GetLogTerm(i) != reply.XTerm {
										continue
									}
									for i < logSize && rf.GetLogTerm(i) == reply.XTerm {
										// leader has XTerm，nextIndex = leader's last entry for XTerm
										i++
									}
									tarIndex = i
								}
							}
							rf.nextIndex[idx] = tarIndex
						}

						rf.mu.Unlock()

					} else {

						rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						rf.updateCommitIndex()
						rf.mu.Unlock()
						return
					}
				}

			}
		}(index)
	}

}

func (rf *Raft) initLeader() {
	// nextIndex: initialized to leaderlast log index + 1
	// matchIndex: initialized to 0, increases monotonically
	for index := range rf.peers {
		rf.nextIndex[index] = rf.GetLastLogIndex() + 1
		rf.matchIndex[index] = 0
	}

	rf.matchIndex[rf.me] = rf.GetLastLogIndex()
}

// check whether newTerm > currentTerm
// if so, convert fo follower
// return true if newTerm > currentTerm
func (rf *Raft) checkTerm(newTerm int64) bool {
	cterm := rf.currentTerm
	if newTerm > cterm {
		rf.currentTerm = newTerm
		rf.persist()
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
	defer rf.mu.Unlock()

	if rf.state != Candidate || rf.currentTerm != args.Term || rf.checkTerm(reply.Term) {
		// the state has changed or the term has changed or reply term > currentTerm
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			// get the majority votes
			rf.toState(Leader)
		}
	}

	return ok
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []Log // empty for heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
	XTerm   int64
	XIndex  int
	Xlen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = 0
	reply.XIndex = 0
	reply.Xlen = 0
	reply.Success = false

	if rf.currentTerm > args.Term {
		// Reply false if term < currentTerm
		reply.Term = rf.currentTerm

		return
	}

	rf.checkTerm(args.Term)
	rf.resetElectionTimer()

	if rf.GetLastLogIndex() < args.PrevLogIndex {
		// if log doesn’t contain an entry at prevLogIndex
		reply.Term = rf.currentTerm
		reply.Xlen = rf.GetLastLogIndex() + 1
		return
	}

	if rf.GetLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Term = rf.currentTerm
		reply.XTerm = rf.GetLogTerm(args.PrevLogIndex)
		index := rf.snapshotIndex
		logSize := rf.GetLastLogIndex() + 1
		for ; index < logSize; index++ {
			if rf.GetLogTerm(index) == rf.GetLogTerm(args.PrevLogIndex) {
				break
			}
		}
		reply.XIndex = index
		return
	}

	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index <= rf.GetLastLogIndex() {
			if rf.GetLogTerm(index) == args.Entries[i].Term {
				continue
			} else {
				rf.log = rf.log[:(index - rf.snapshotIndex)]
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		//  leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.GetLastLogIndex() {
			rf.commitIndex = rf.GetLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		// apply log to state machine
		rf.applyLog()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndex() {
	for i := rf.commitIndex + 1; i <= rf.GetLastLogIndex(); i++ {
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
		if rf.GetLogTerm(i) == rf.currentTerm {
			count := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= i {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = i
			}
		}
	}

	rf.applyLog()
}

func (rf *Raft) applyLog() {
	if rf.lastApplied == rf.commitIndex || rf.GetLogTerm(rf.commitIndex) < rf.currentTerm {
		return
	}
	entries := make([]Log, rf.commitIndex-rf.lastApplied)
	copy(entries, rf.log[(rf.lastApplied-rf.snapshotIndex+1):(rf.commitIndex-rf.snapshotIndex+1)])
	temp := rf.commitIndex
	rf.mu.Unlock() //create a snapshot, release the lock, or it will deadlock

	for _, log := range entries {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index,
		}
		rf.applyCh <- applyMsg
	}
	rf.mu.Lock()
	if rf.lastApplied < temp {
		rf.lastApplied = temp
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) { // TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	// Your code here (2B).
	newIndex := rf.GetLastLogIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, Log{term, newIndex, command})
	rf.persist()
	// update nextIndex and matchIndex
	rf.nextIndex[rf.me] = rf.GetLastLogIndex() + 1
	rf.matchIndex[rf.me] = rf.GetLastLogIndex()

	rf.leaderTimer.Reset(time.Microsecond)

	// return index, term, isLeader
	return newIndex, int(term), true
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.log = append([]Log{}, Log{0, 0, nil})
	rf.snapshotTerm = 0
	rf.snapshotIndex = 0

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// volatile state on leaders
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.electionTimer = time.NewTimer(time.Hour)
	rf.leaderTimer = time.NewTimer(time.Hour)
	go rf.startElectionTimer()
	go rf.startHeartBeat()

	rf.mu.Lock()
	rf.toState(Follower)
	rf.mu.Unlock()

	return rf
}
