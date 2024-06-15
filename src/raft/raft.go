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
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const HeartbeatTime = 150
const ElectionTimeoutRangeBottom = 200
const ElectionTimeoutRangeTop = 450

func RandomElectionTimeout() int {
	return ElectionTimeoutRangeBottom + rand.Intn(ElectionTimeoutRangeTop-ElectionTimeoutRangeBottom)
}

func HeartbeatTimeThreshold() int {
	// return (int)(HeartbeatTime * 1.5) // + rand.Intn(HeartbeatTime)
	return HeartbeatTime + rand.Intn(HeartbeatTime)
}

const (
	Leader int = iota
	Follower
	Candidate
)

type Entry struct {
	Term    int
	Command interface{}
}

func roleName(idx int) string {
	switch idx {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

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
	applyCh     chan ApplyMsg
	log         []Entry // 日志
	currentTerm int     // 最新任期
	votedFor    int     // 收到的投票请求的候选者Id

	commitIndex int // 已提交的最高Index
	// lastApplied int // 提交到状态机的最高Index
	// 选举后需要重新初始化的:
	nextIndex  []int //
	matchIndex []int

	heartbeatTimeStamp time.Time
	electionTimeStamp  time.Time // 记录收到消息时的时间戳
	electionTimeout    int
	role               int
	voteCount          int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// Reading don't need lock
	isleader = rf.role == Leader
	term = rf.currentTerm
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
	Term         int // 候选者的任期
	CandidateId  int
	LastLogIndex int // 候选人最后一个日志的下标
	LastLogTerm  int // 候选人最后一个日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期，用于候选人更新
	VoteGranted bool // 候选人是否收到投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// log.Printf("%v(V: %v) << %v's requestVote", rf.me, rf.votedFor, args.CandidateId)
	rf.mu.Lock()
	log.Printf("%v(T: %v, V: %v) <<< %v(T: %v)", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
	// defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 1. Candidate 的任期小于 Follower 任期
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("%v(T: %v, V: %v)  X  %v(T: %v)\n", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		// 新一轮投票，需要取消上一轮的投票
		rf.votedFor = -1
		rf.currentTerm = args.Term // 需要将自己的Term更新，以防止再次开启一轮投票
		rf.role = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// if votedFor is null or candidateId
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm >= rf.log[len(rf.log)-1].Term) {
			// 需要防止有旧log的candidate选举成功，从而覆盖其他log
			// and candidate's log is at least as up-to-date as receiver's log, grant vote
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.heartbeatTimeStamp = time.Now()

			rf.mu.Unlock()
			reply.VoteGranted = true
			log.Printf("%v(T: %v, V: %v)  V  %v(T: %v)\n", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
			return
		}
	}
	log.Printf("%v(T: %v, V: %v)  X  %v(T: %v)\n", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     //follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry //log entries to store (empty for heartbeat, more than one for efficiency)
	LeaderCommit int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	// 重置Follower的心跳时间
	rf.heartbeatTimeStamp = time.Now()

	if args.Term > rf.currentTerm {
		// 新Leader的消息
		log.Printf("New Leader: %v(T: %v) Follower: %v(T: %v)\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		rf.currentTerm = args.Term // 更新Term
		rf.votedFor = -1           // 新Leader已经产生，消除之前的投票记录
		rf.role = Follower         // 心跳抑制投票
	}

	if args.Entries == nil {
		// 心跳

		// log.Printf("Follower server_%v (Term: %v) receives Leader server_%v 's heartbeat (Term: %v)\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}

	if args.Entries != nil && // 收到的 Entries 不空
		(args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// 2. Reply false log doesn't contain an entry at prevLogIndex whose term mathces prevLogTerm
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	// 3. An existing entry conflicts with a new one(same index but different term)
	// TODO: delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		// 5. LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = (int)(math.Min((float64)(args.LeaderCommit), (float64)(len(rf.log)-1)))
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatToServer(server int, args *AppendEntriesArgs) {
	// log.Printf("server_%v send heartbeat to server_%v\n", rf.me, server)
	reply := &AppendEntriesReply{}
	sendArgs := &args
	ok := rf.sendAppendEntries(server, *sendArgs, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (*sendArgs).Term != rf.currentTerm {
		// 可能发送心跳期间，任期更改
		return
	}
	if reply.Term > rf.currentTerm {
		log.Printf("Old Leader: %v(T: %v) received a new Reply: (T: %v), convert to Follower", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}
}

func (rf *Raft) SendHeartbeats() {
	// log.Printf("server_%v tries to send heartbeat\n", rf.me)
	for !rf.killed() {
		// 只要没有被kill掉就定期持续发送
		// log.Printf("server_%v tries to send heartbeat and get in loop\n", rf.me)
		rf.mu.Lock()
		// log.Printf("server_%v tries to send heartbeat and get lock\n", rf.me)
		// 不是Leader不能发心跳
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go rf.sendHeartbeatToServer(i, args)
		}
		// 睡眠一个心跳间隔
		time.Sleep(time.Duration(HeartbeatTime) * time.Millisecond)
	}
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

func (rf *Raft) procVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := &args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, *sendArgs, &reply)
	if !ok { // 调用失败，直接返回投票失败
		// log.Printf("cannot connect to server_%v", server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (*sendArgs).Term != rf.currentTerm {
		// 发送投票请求时，有其他Leader产生了，并通过心跳改变了自己的任期，需要放弃投票
		return false
	}

	if reply.Term > rf.currentTerm {
		// Follower 任期大于 Candidate，需要更新自己记录的当前任期、清除投票、改变角色
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	return reply.VoteGranted
}

func (rf *Raft) collectVote(server int, args *RequestVoteArgs) {
	ok := rf.procVoteAnswer(server, args)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		// 如果投票数已经超过了半数，直接返回，因为之前的协程已经处理过了
		rf.mu.Unlock()
		return
	}
	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 && rf.role == Candidate && time.Since(rf.electionTimeStamp) <= time.Duration(rf.electionTimeout)*time.Millisecond {
		// 第一次超过半票，并且需要检查自己的身份还是否为Candidate，因为期间可能有其他Leader产生
		// 需要成为leader，并发送心跳
		rf.role = Leader
		log.Printf("Server_%v becomes new Leader", rf.me)
		rf.mu.Unlock()
		// TODO: 发送心跳消息
		go rf.SendHeartbeats()
	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) StartElection() {

	rf.mu.Lock()
	// log.Printf("Follower server_%v start a requestVote\n", rf.me)
	rf.currentTerm += 1 // 自增Term
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.electionTimeout = RandomElectionTimeout()
	rf.electionTimeStamp = time.Now()  // 更新自己的选举时间戳
	rf.heartbeatTimeStamp = time.Now() // 以免当前选举还未结束，自己又开启一轮选举

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		// log.Printf("%v server_%v checking itself\n", roleName(rf.role), rf.me)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.heartbeatTimeStamp) > time.Duration(HeartbeatTimeThreshold())*time.Millisecond {
			// TODO: 发起选举
			// log.Printf("server_%v shoud start a requestVote\n", rf.me)
			go rf.StartElection()
		}
		// else {
		// 	log.Printf("%v server_%v time.Since(rf.heartbeatTimeStamp)=%v, time.Duration(HeartbeatTimeThreshold())*time.Millisecond = %v\n", roleName(rf.role), rf.me, time.Since(rf.heartbeatTimeStamp), time.Duration(HeartbeatTimeThreshold())*time.Millisecond)
		// }

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,
		applyCh:   applyCh,
		dead:      0,

		log:                make([]Entry, 0),
		currentTerm:        0,
		votedFor:           -1,
		nextIndex:          make([]int, len(peers)),
		matchIndex:         make([]int, len(peers)),
		role:               Follower,
		heartbeatTimeStamp: time.Now(),
		electionTimeStamp:  time.Now(),
		voteCount:          0,
	}
	rf.log = append(rf.log, Entry{Term: 0})
	// rf.peers = peers
	// rf.persister = persister
	// rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
