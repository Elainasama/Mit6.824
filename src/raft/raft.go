package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, currentTerm, isleader)
//   start agreement on a new logs entry
// rf.GetState() (currentTerm, isLeader)
//   ask a Raft for its current currentTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "labs-6.824/src/labrpc"

// ApplyMsg as each Raft peer becomes aware that successive logs Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 定期将日志提交
// 并行化处理时考虑加锁，防止资源篡改。
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// figure8 简化
	// Leader不能直接提交不属于自己任期的日志。
	if rf.logs[rf.commitIndex].Term < rf.currentTerm {
		return
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: rf.logs[i].Index,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) doApplyWork() {
	for rf.dead == 0 {
		rf.applyLog()
		time.Sleep(commitInterval)
	}
}

// leader检查commitIndex并率先进行修改
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) checkCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for idx := len(rf.logs) - 1; idx >= rf.commitIndex; idx-- {
		if rf.role != Leader {
			return
		}
		cnt := 1
		for i := range rf.matchIndex {
			if i != rf.me && rf.matchIndex[i] >= idx {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			if rf.role == Leader {
				rf.commitIndex = idx
			}
			break
		}
	}
}

// Raft
// 2A Leader Election
// 2B Append Log Entries
// 如果不通过应该仔细翻看论文，观察细节的地方，论文条理写的很清楚。
// done figure8
// todo 不一致的快速回退
// done -race 测试
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role              RoleType      // 记录节点目前状态
	currentTerm       int           // 节点当前任期
	votedFor          int           // follower把票投给了哪个candidate
	voteCount         int           // 记录所获选票的个数
	appendEntriesChan chan struct{} // 心跳channel
	LeaderMsgChan     chan struct{} // 当选Leader时发送
	VoteMsgChan       chan struct{} // 收到选举信号时重置一下计时器，不然会出现覆盖term后计时器超时又突然自增。

	// 2B
	commitIndex int           // Index of highest logs entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int           // Index of highest logs entry applied to state machine (initialized to 0, increases monotonically)
	logs        []LogEntries  // logs Entries; each entry contains Command for state machine, and Term when entry was received by leader (first Index is 1)
	nextIndex   []int         // for each server, Index of the next logs entry to send to that server (initialized to leader last logs Index + 1)
	matchIndex  []int         //  for each server, Index of highest logs entry known to be replicated on server (initialized to 0, increases monotonically)
	applyChan   chan ApplyMsg // 提交给客户端已完成半数复制的log
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.role == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// RequestVoteArgs example RequestVoteHandler RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s Term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //Index of candidate’s last logs entry (§5.4)
	LastLogTerm  int //currentTerm of candidate’s last logs entry (§5.4)
}

// RequestVoteReply example RequestVoteHandler RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// RequestVoteHandler example RequestVoteHandler RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 设置返回的任期，投票默认返回false。
	reply.Term = rf.currentTerm
	// 这里不加return 因为一个candidate一轮只发送一次选举。Follower收到了修改自己任期即可。
	// 后面可以继续参与投票。
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		rf.VoteMsgChan <- struct{}{}
	}
	// 双向影响，不接受任期小的Candidate的投票申请
	if args.Term < rf.currentTerm {
		return
	}

	// Reply false if currentTerm < currentTerm
	// candidate’s logs is at least as up-to-date
	// 这里的比较逻辑原先理解为是commit更多的优先当选 重看一遍论文才发现原来是term更大的log更长的优先当选。
	lastLog := rf.logs[len(rf.logs)-1]
	if args.LastLogTerm < lastLog.Term || args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index {
		return
	}

	// If votedFor is null or candidateId, and candidate’s logs is at least as up-to-date as receiver’s logs, grant vote
	if rf.role == Follower && (rf.votedFor == noVoted || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.VoteMsgChan <- struct{}{}
	}
}

// example code to send a RequestVoteHandler RPC to a server.
// server is the Index of the target server in rf.peers[].
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
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	// 发送失败直接返回
	if !ok {
		return false
	}

	//并发下加锁保平安~
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果已经不是candidate了，无须继续拉票。
	if rf.role != Candidate || args.Term != rf.currentTerm {
		return true
	}
	// 遇到了任期比自己大的节点，转为follower
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
		rf.VoteMsgChan <- struct{}{}
		return true
	}
	if reply.VoteGranted && rf.role == Candidate {
		rf.voteCount++
		if 2*rf.voteCount > len(rf.peers) && rf.role == Candidate {
			rf.ConvertToLeader()
			// 超半数票 直接当选
			rf.LeaderMsgChan <- struct{}{}
		}
	}
	return true
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// CurrentTerm. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.role != Leader {
		return -1, -1, false
	}
	newLog := rf.appendLog(command)
	term = newLog.Term
	index = newLog.Index
	return index, term, isLeader
}

func (rf *Raft) appendLog(command interface{}) LogEntries {
	newLog := LogEntries{
		Command: command,
		Term:    rf.currentTerm,
		Index:   len(rf.logs),
	}
	rf.logs = append(rf.logs, newLog)
	return newLog
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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

// Make the service or tester wants to create a Raft server. the ports
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
		mu:                sync.Mutex{},
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		role:              Follower,
		currentTerm:       0,
		votedFor:          noVoted,
		voteCount:         0,
		appendEntriesChan: make(chan struct{}, chanLen),
		LeaderMsgChan:     make(chan struct{}, chanLen),
		VoteMsgChan:       make(chan struct{}, chanLen),
		commitIndex:       0,
		lastApplied:       0,
		logs:              []LogEntries{{}},
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		applyChan:         applyCh,
	}
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()
	go rf.doApplyWork()
	return rf
}

func (rf *Raft) ConvertToFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = noVoted
	rf.voteCount = 0
}

func (rf *Raft) ConvertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Candidate
	// 自身任期自增
	rf.currentTerm++
	//  投票给自己
	rf.votedFor = rf.me
	rf.voteCount = 1
}
func (rf *Raft) ConvertToLeader() {
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// 重置nextIndex和matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
}

func (rf *Raft) Run() {
	// dead置1则退出运行
	for rf.dead == 0 {
		//fmt.Println(rf.me, rf.role, rf.currentTerm, rf.logs)
		//fmt.Println(rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.voteCount)
		switch rf.role {
		case Candidate:

			go rf.sendAllRequestVote()
			select {
			case <-rf.VoteMsgChan:
				continue
			case <-rf.appendEntriesChan:

			case <-time.After(electionTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// 选举超时 重置选举状态
				rf.ConvertToCandidate()
				continue
			case <-rf.LeaderMsgChan:
			}

		case Leader:
			// Leader 定期发送心跳和同步日志
			rf.SendAllAppendEntries()
			// 更新commitIndex对子节点中超过半数复制的日志进行提交
			rf.checkCommitIndex()
			time.Sleep(heartBeatInterval)
		case Follower:
			select {
			case <-rf.VoteMsgChan:
				continue
			case <-rf.appendEntriesChan:
				continue
			case <-time.After(appendEntriesTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// 增加扰动避免多个Candidate同时进入选举
				rf.ConvertToCandidate()
			}
		}
	}
}

type LogEntries struct {
	Command interface{}
	Term    int
	Index   int
}

// AppendEntriesArgs Tips
// 根据raft的实现，cmd提交必须经过2次心跳leader发送新log，并收到大多数follower的确认，
// leader更新commitIndex并提交logleader将自己的commitIndex发送给follower
// follower更新commitIndex并提交log
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntries // logs Entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int          // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// SendAllAppendEntries 由Leader向其他所有节点调用来复制日志条目;也用作heartbeat
func (rf *Raft) SendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.role == Leader {
			// 向follower发送nextIndex最新log日志
			// 如果无需更新 则发送心跳即可。
			// 为了效率 可以一次发送多份
			// 这一段要在锁内处理，防止越界。
			nxtId := rf.nextIndex[server]
			lastLog := rf.logs[nxtId-1]
			logs := make([]LogEntries, len(rf.logs)-nxtId)
			copy(logs, rf.logs[nxtId:])
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				LeaderCommit: rf.commitIndex,
				Entries:      logs,
			}
			go func(id int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{
					Term:    0,
					Success: false,
				}
				rf.SendAppendEntries(id, args, reply)
			}(server, args)
		}
	}
}

func (rf *Raft) SendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[id].Call("Raft.AppendEntriesHandler", args, reply)
	// 发送失败直接返回即可。
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
	}

	if rf.role != Leader {
		return
	}
	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	if !reply.Success {
		rf.nextIndex[id]--
	} else {
		rf.nextIndex[id] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[id])
		rf.matchIndex[id] = rf.nextIndex[id] - 1
	}

}

// AppendEntriesHandler 除Leader以外其余节点的处理逻辑
// 在Raft中，领导者通过强迫追随者的日志复制自己的日志来处理不一致。
// 这意味着跟随者日志中的冲突条目将被来自领导者日志的条目覆盖。
// 第5.4节将说明，如果加上另外一个限制，这样做是安全的。
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 传一个空结构体表示接收到了Leader的请求。
	// 收到Leader更高的任期时，更新自己的任期。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// 老Leader重连后Follower不接受旧信号
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.ConvertToFollower(args.Term)
	}
	// 发送心跳重置计时器
	rf.appendEntriesChan <- struct{}{}
	if args.PrevLogIndex >= len(rf.logs) {
		return
	}
	lastLog := rf.logs[args.PrevLogIndex]
	// 最后的日志对不上 因此需要让Leader对该节点的nextIndex - 1。
	if args.PrevLogTerm != lastLog.Term {
		return
	}
	// 在PrevLogIndex处开始复制一份日志
	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	rf.commitIndex = min(len(rf.logs)-1, args.LeaderCommit)
	reply.Success = true
}

func (rf *Raft) sendAllRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.logs[len(rf.logs)-1]
	arg := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for i := range rf.peers {
		if i != rf.me && rf.role == Candidate {
			go func(id int) {
				ret := &RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}
				rf.sendRequestVote(id, arg, ret)
			}(i)
		}
	}
}
