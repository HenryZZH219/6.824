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
	"6.5840/labgob"
	"bytes"
	"fmt"

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

	//persistent
	currentTerm int        //服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int        //在当前获得选票的候选人的 Id
	logs        []LogEntry //日志条目集

	//volatile
	commitIndex int
	lastApplied int

	//volatile for leaders
	nextIndex  []int
	matchIndex []int

	//myown
	roll       Roll
	timeticker *time.Ticker
	overtime   time.Duration

	applyChan chan ApplyMsg
}

type Roll int

const (
	leader Roll = iota
	follower
	candidate
)

const HeartBeatTime = time.Duration(120 * time.Millisecond)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	isleader = rf.roll == leader

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type failreason int

const (
	notfail failreason = iota
	expired
)

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term         int
	VoteGranted  bool
	FailedReason failreason
	LenLogs      int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 追加新的日志的任期号(这边传的应该都是当前leader的任期号 args初始化为：rf.currentTerm
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index

	//我自己的
	EndLogIndex int
}

type AppendEntriesState int

type AppendEntriesReply struct {
	Term    int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false

	UpNextIndex int
	AppState    AppendEntriesState // 追加状态
	LenLogs     int
}

const (
	logError AppendEntriesState = iota
	termExpired
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("[func-show-all]: id:%+v, term:%+v, roll:%+v, votefor:%+v\n", rf.me, rf.currentTerm, rf.roll, rf.votedFor)
	//fmt.Printf("[func-RequestVote-rf(%+v)] : get message from:%+v\n", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.FailedReason = expired
		reply.LenLogs = len(rf.logs)
		//fmt.Printf("[func-RequestVote-rf(%+v)] : return:%+v， args term:%+v\n", rf.me, reply.VoteGranted, args.Term)
		return
	}

	//log
	if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Term > args.LastLogTerm || len(rf.logs)-1 > args.LastLogIndex && rf.logs[len(rf.logs)-1].Term == args.LastLogTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.FailedReason = expired
		reply.LenLogs = len(rf.logs)
		//fmt.Printf("[func-RequestVote-rf(%+v)] : cuttrnt:%+v， args:%+v\n", rf.me, rf, args)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.roll = follower
		rf.votedFor = -1
	}

	if rf.votedFor == -1 {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		reply.LenLogs = len(rf.logs)
		rf.votedFor = args.CandidateId
		rf.timeticker.Reset(rf.overtime)
		rf.persist()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.LenLogs = len(rf.logs)
		if rf.votedFor == args.CandidateId {
			rf.timeticker.Reset(rf.overtime)
		}
	}

	//fmt.Printf("[func-RequestVote-rf(%+v)] : return:%+v, my term:%d, args term%v \n", rf.me, reply.VoteGranted, rf.currentTerm, args.Term)
	return

}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	//fmt.Printf("[func-show-all]: id:%+v, term:%+v, roll:%+v, votefor:%+v\n", rf.me, rf.currentTerm, rf.roll, rf.votedFor)
	//fmt.Printf("[func-AppendEntries-rf(%+v)] : get message from:%+v\n", rf.me, args.LeaderId)
	//fmt.Printf("[	AppendEntries func-rf(%v)	] log information:len entries:%v, prelogindex  %v\n", rf.me, len(args.Entries), args.PrevLogIndex)
	reply.Term = rf.currentTerm
	//1如果 term < currentTerm 就返回 false （5.1 节）
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.AppState = termExpired
		//323fmt.Printf("[func-AppendEntries-rf(%+v)] : get message:%+v,  my term:%d, args term%v, from %v\n", rf.me, reply.Success, rf.currentTerm, args.Term, args.LeaderId)
		return
	}
	//如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
	//如果已经已经存在的日志条目和新的产生冲突（相同偏移量但是任期号不同），删除这一条和之后所有的 （5.3 节）
	//附加任何在已有的日志中不存在的条目
	//如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个

	//2-5
	//fmt.Printf("[func-AppendEntries-rf(%+v)] : prelogindex%+v, lenlogs%v\n", rf.me, args.PrevLogIndex, len(rf.logs))
	if args.PrevLogIndex != -1 && args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		//323fmt.Printf("[func-AppendEntries-rf(%+v)] : logs mismatch:%+v,  log lens:%d, log term%v, args term%v \n", rf.me, reply.Success, rf.currentTerm, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.AppState = logError
		return
	}

	//如果超前状态 感觉这个没用 先不写了
	//if len(rf.logs) > args.PrevLogIndex+1 {
	//
	//}

	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.roll = follower
	rf.timeticker.Reset(rf.overtime)

	reply.Success = true
	reply.Term = rf.currentTerm
	//fmt.Printf("[func-AppendEntries-rf(%+v)] : get message:%+v\n", rf.me, reply.Success)

	if args.Entries != nil {
		//if args.PrevLogIndex+len(args.Entries) == len(rf.logs) {
		//	//123fmt.Printf("[	AppendEntries func-rf(%v)	] copy log， but no need log:%v  \n", rf.me, len(rf.logs))
		//} else {
		//	//123fmt.Printf("[	AppendEntries func-rf(%v)	] copy log， log:%v  \n", rf.me, len(rf.logs))
		//	rf.logs = rf.logs[:args.PrevLogIndex+1]
		//	rf.logs = append(rf.logs, args.Entries...)
		//}
		//123fmt.Printf("[	AppendEntries func-rf(%v)	] copy log， log:%v, %v  \n", rf.me, len(rf.logs), len(args.Entries))
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)

	}
	//323fmt.Printf("[	AppendEntries func-rf(%v)	] success from:%v  \n", rf.me, rf.votedFor)
	for rf.lastApplied <= args.LeaderCommit {
		//323fmt.Printf("[	AppendEntries func-rf(%v)	] commitLog from follower， log:%v  \n", rf.me, rf.lastApplied)
		applyMsh := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		//323fmt.Printf("%+v\n", applyMsh)
		rf.applyChan <- applyMsh
		rf.commitIndex = rf.lastApplied
		rf.lastApplied++
	}

	reply.UpNextIndex = len(rf.logs)
	rf.persist()
	//323fmt.Printf("[	AppendEntries func-rf(%v)	] %v， %v, from%v\n", rf.me, rf.lastApplied, args.LeaderCommit, args.LeaderId)
	//323fmt.Printf("[	AppendEntries func-rf(%v)	] lenlogs %v\n", rf.me, len(rf.logs))
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteCounts *int) bool {

	//fmt.Printf("[func-sendRequestVote-rf(%+v)] : send to %+v\n", rf.me, server)

	ok := false
	loopCount := 0
	for ok == false {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		if loopCount > 10 {
			return ok
		}
		loopCount++
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		//fmt.Printf("[func-sendRequestVote-rf(%+v)] : get one vote from:%+v, result:%+v, total %+v, vote expired\n", rf.me, server, reply.VoteGranted, *voteCounts)
		return false
	}

	if reply.VoteGranted == true {
		*voteCounts += 1
		//323fmt.Printf("[func-sendRequestVote-rf(%+v)] : get one vote from:%+v, total %+v, term%v, success%+v\n", rf.me, server, *voteCounts, rf.currentTerm, reply)
	} else if reply.FailedReason == expired {
		//323fmt.Printf("[func-sendRequestVote-rf(%+v)] : get one vote from:%+v, failed %+v\n", rf.me, server, reply)
		rf.roll = follower
		rf.overtime = time.Duration(350+rand.Intn(200)) * time.Millisecond
		rf.timeticker.Reset(rf.overtime)

		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
	} else {
		//323fmt.Printf("[func-sendRequestVote-rf(%+v)] : get one vote from:%+v, failed for no reason%+v\n", rf.me, server, reply)
	}

	if *voteCounts == len(rf.peers)/2+1 {
		if rf.roll != leader {
			rf.roll = leader
			rf.timeticker.Reset(HeartBeatTime)

			//rf.nextIndex = make([]int, len(rf.peers))
			//for i, _ := range rf.nextIndex {
			//	rf.nextIndex[i] = len(rf.logs)
			//}
			//323fmt.Printf("[func-sendRequestVote-rf(%+v)] : become leader, total votes:%+v, term:%v\n", rf.me, *voteCounts, rf.currentTerm)
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendCounts *int) bool {

	//123fmt.Printf("[func-sendAppendEntries-rf(%+v)] : sending to%+v, on log %v\n", rf.me, server, rf.nextIndex[server])
	ok := false
	loopCount := 0
	for ok == false {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if loopCount > 10 {
			return ok
		}
		loopCount++
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm || reply.Term != rf.currentTerm {
		//123fmt.Printf("[func-sendAppendEntries-rf(%+v)] : EXPIRED MESSAGE ； sucecess or not%+v\n", rf.me, ok)
		return false
	}
	//fmt.Printf("[func-sendAppendEntries-rf(%+v)] : sucecess or not%+v\n", rf.me, ok)
	if reply.Success == true {
		*appendCounts += 1
		rf.nextIndex[server] = reply.UpNextIndex
		//323fmt.Printf("[\tsendAppendEntries func-rf(%v)\t] : sendAppendEntries success from:%+v, total %+v, term%v\n", rf.me, server, *appendCounts, args.Term)
	} else {
		if reply.AppState == logError {
			rf.nextIndex[server]--
			//123fmt.Printf("[\tsendAppendEntries func-rf(%v)\t] : sendAppendEntries fail from:%+v, total %+v\n", rf.me, server, *appendCounts)

			if rf.nextIndex[server] < 0 {
				//123fmt.Printf("[\tsendAppendEntries func-rf(%v)\t] : FATAL ERROR!!!!! <0\n", rf.me)
			}
			return false
		} else if reply.AppState == termExpired {
			//感觉不需要处理其实
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.roll = follower
			rf.timeticker.Reset(rf.overtime)
			return false
		}
	}

	//if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
	//	return false
	//}

	if *appendCounts == len(rf.peers)/2+1 {
		//lastApplied = len(logs) commitIndex = len - 1
		for rf.lastApplied < args.EndLogIndex { //len(rf.logs)

			//323fmt.Printf("[	sendAppendEntries func-rf(%v)	] commitLog from leader， log:%v  \n", rf.me, rf.lastApplied)
			applyMsh := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			//323fmt.Printf("%+v\n", applyMsh)
			rf.applyChan <- applyMsh
			rf.commitIndex = rf.lastApplied
			rf.lastApplied++
		}
		rf.persist()
	}
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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roll != leader {
		return 0, 0, false
	}

	newLog := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, newLog)
	//123fmt.Printf("[	Start func-rf(%v)	] loadCommand， loglenth:%v  \n", rf.me, len(rf.logs))
	return len(rf.logs), rf.currentTerm, true
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
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.timeticker.C:
			if rf.killed() == true {
				return
			}
			switch rf.roll {
			case leader:
				appendCounts := 1
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: -1,
							PrevLogTerm:  -1,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,

							EndLogIndex: len(rf.logs),
						}
						if rf.nextIndex[i] > 0 { // && len(rf.logs) > rf.nextIndex[i]-1
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

						}

						args.Entries = rf.logs[rf.nextIndex[i]:]
						//if rf.nextIndex[i] != len(rf.logs) { //&& rf.nextIndex[i] < len(rf.logs)
						//
						//}

						//
						reply := AppendEntriesReply{}
						//if args.Entries != nil {
						//	//323fmt.Printf("[     func-ticker-rf(%+v)     ] : hearbeat, ALL:%+v\n", rf.me, rf)
						//	//323fmt.Printf("%+v\n", args)
						//}
						//123fmt.Printf("[     func-ticker-rf(%+v)     ] : to%v\n", rf.me, i)
						//123fmt.Printf("%+v\n", args)
						go rf.sendAppendEntries(i, &args, &reply, &appendCounts)
					}
				}

				//323fmt.Printf("[     func-ticker-rf(%+v)     ] : hearbeat, term%v, lenlogs%v\n", rf.me, rf.currentTerm, len(rf.logs))
			case follower:
				//fmt.Printf("[func-ticker-rf(%+v)] : new candidate\n", rf.me)
				rf.roll = candidate
				fallthrough
			case candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.overtime = time.Duration(350+rand.Intn(200)) * time.Millisecond
				rf.timeticker.Reset(rf.overtime)
				voteCounts := 1
				//fmt.Printf("[           func-ticker-rf(%+v)] : new candidate, term:%+v\n", rf.me, rf.currentTerm)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {

						args := RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: len(rf.logs) - 1,
							LastLogTerm:  1,
						}
						if args.LastLogIndex >= 0 {
							args.LastLogTerm = rf.logs[args.LastLogIndex].Term
						}
						//type RequestVoteArgs struct {
						//	// Your data here (3A, 3B).
						//	term         int
						//	candidateId  int
						//	lastLogIndex int
						//	lastLogTerm  int
						//}
						reply := RequestVoteReply{}
						rf.persist()
						go rf.sendRequestVote(i, &args, &reply, &voteCounts)
					}
				}
			}
		}

		//fmt.Printf("[func-show-all]: id:%+v, term:%+v, roll:%+v, votefor:%+v\n", rf.me, rf.currentTerm, rf.roll, rf.votedFor)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1 //在当前获得选票的候选人的 Id
	rf.logs = make([]LogEntry, 0)

	//volatile
	rf.commitIndex = -1
	rf.lastApplied = 0

	//volatile for leaders
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//myown
	rf.roll = follower
	rf.overtime = time.Duration(550+rand.Intn(200)) * time.Millisecond // 随机产生150-350ms
	rf.timeticker = time.NewTicker(rf.overtime)

	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//123fmt.Printf("[func-Make-rf(%+v)] : init\n", rf.me)

	return rf
}

//currentTerm int       //服务器最后一次知道的任期号（初始化为 0，持续递增）
//votedFor    int       //在当前获得选票的候选人的 Id
//logs        []LogSets //日志条目集
//
////volatile
//commitIndex int
//lastApplied int
//
////volatile for leaders
//nextIndex  int
//matchIndex int
//
////myown
//roll       Roll
//timeticker time.Ticker
//overtime   time.Duration
