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
	"bytes"
	"labs-6.824/src/labgob"
	"log"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 定期将日志提交
// 并行化处理时考虑加锁，防止资源篡改。
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	SnapShotIndex := rf.getFirstLog().Index
	// 此时断了连接 commitIndex从0恢复需要执行一次checkCommitIndex。
	if rf.commitIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}
	copyLogs := make([]LogEntries, rf.commitIndex-rf.lastApplied)
	copy(copyLogs, rf.logs[rf.lastApplied-SnapShotIndex+1:rf.commitIndex-SnapShotIndex+1])
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()
	// 这里不要加锁 2D测试函数会死锁
	for _, logEntity := range copyLogs {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      logEntity.Command,
			CommandIndex: logEntity.Index,
		}
	}
}

func (rf *Raft) doApplyWork() {
	for !rf.killed() {
		rf.applyLog()
		time.Sleep(commitInterval)
	}
}

// leader检查commitIndex并率先进行修改
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].Term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) checkCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapShotIndex := rf.getFirstLog().Index
	for idx := len(rf.logs) - 1; idx >= rf.commitIndex-snapShotIndex; idx-- {
		// figure8 简化
		// Leader不能直接提交不属于自己任期的日志。
		if rf.logs[idx].Term < rf.currentTerm || rf.role != Leader {
			return
		}
		cnt := 1
		for i := range rf.matchIndex {
			if i != rf.me && rf.matchIndex[i] >= idx+snapShotIndex {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			if rf.role == Leader {
				rf.commitIndex = idx + snapShotIndex
			}
			break
		}
	}
}

// Raft
// 2A Leader Election
// 2B Append Log Entries
// 2C Persistence 在处理RPC请求时增加持久化处理即可
// 2D Log Compaction(Snapshot) 新增一个下标偏移以及快照复制的功能
// 如果不通过应该仔细翻看论文，观察细节的地方，论文条理写的很清楚。
// Done figure8
// Done 不一致的快速回退
// todo -race 测试
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role          RoleType      // 记录节点目前状态
	currentTerm   int           // 节点当前任期
	votedFor      int           // follower把票投给了哪个candidate
	voteCount     int           // 记录所获选票的个数
	HeartBeatChan chan struct{} // 心跳channel
	LeaderMsgChan chan struct{} // 当选Leader时发送
	VoteMsgChan   chan struct{} // 收到选举信号时重置一下计时器，不然会出现覆盖term后计时器超时又突然自增。

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
	// Data := w.Bytes()
	// rf.persister.SaveRaftState(Data)

	rf.persister.SaveRaftState(rf.EncoderState())
}

func (rf *Raft) EncoderState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 需要保存的内容
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil {
		log.Fatal("Errors occur when Encoder")
	}
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(Data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntries
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("errors occur when Decoder")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		rf.lastApplied = rf.getFirstLog().Index
	}
}

// RequestVoteArgs example RequestVoteHandler RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your Data here (2A, 2B).
	Term         int //candidate’s Term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //Index of candidate’s last logs entry (§5.4)
	LastLogTerm  int //currentTerm of candidate’s last logs entry (§5.4)
}

// RequestVoteReply example RequestVoteHandler RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your Data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// RequestVoteHandler example RequestVoteHandler RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,Term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	// 设置返回的任期，投票默认返回false。
	reply.Term = rf.currentTerm
	// 这里不加return 因为一个candidate一轮只发送一次选举。Follower收到了修改自己任期即可。
	// 后面可以继续参与投票。
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		// 重置选票
		rf.votedFor = noVoted
	}
	// 双向影响，不接受任期小于的Candidate的投票申请
	if args.Term < rf.currentTerm {
		return
	}

	// Reply false if currentTerm < currentTerm
	// candidate’s logs is at least as up-to-date
	// 这里的比较逻辑原先理解为是commit更多的优先当选 重看一遍论文才发现原来是term更大的log更长的优先当选。
	lastLog := rf.getLastLog()
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
	defer rf.persist()
	defer DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in Term %v", rf.me, args, server, reply, rf.currentTerm)

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
		Index:   rf.getLastLog().Index + 1,
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
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		role:          Follower,
		currentTerm:   0,
		votedFor:      noVoted,
		voteCount:     0,
		HeartBeatChan: make(chan struct{}, chanLen),
		LeaderMsgChan: make(chan struct{}, chanLen),
		VoteMsgChan:   make(chan struct{}, chanLen),
		commitIndex:   0,
		lastApplied:   0,
		logs:          []LogEntries{{}},
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		applyChan:     applyCh,
	}
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()
	go rf.doApplyWork()
	return rf
}

func (rf *Raft) ConvertToFollower(term int) {
	rf.currentTerm = term
	rf.role = Follower
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
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// 重置nextIndex和matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLog().Index + 1
	}
	rf.role = Leader
	// 发送no-op日志
	//rf.appendLog(nil)
}

func (rf *Raft) Run() {
	// dead置1则退出运行
	for !rf.killed() {
		// fmt.Println(rf.me, rf.role, rf.currentTerm, rf.logs, rf.votedFor, rf.voteCount, rf.nextIndex)
		// fmt.Println(rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.voteCount)
		switch rf.role {
		case Candidate:

			select {
			case <-rf.VoteMsgChan:

			case <-rf.HeartBeatChan:

			case <-time.After(electionTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// 选举超时 重置选举状态
				if rf.role == Candidate {
					rf.ConvertToCandidate()
					go rf.sendAllRequestVote()
				}

			case <-rf.LeaderMsgChan:
			}

		case Leader:
			// Leader 定期发送心跳和同步日志
			rf.SendAllAppendEntries()
			// 更新commitIndex对子节点中超过半数复制的日志进行提交
			go rf.checkCommitIndex()
			time.Sleep(heartBeatInterval)
		case Follower:
			select {
			case <-rf.VoteMsgChan:

			case <-rf.HeartBeatChan:

			case <-time.After(appendEntriesTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// 增加扰动避免多个Candidate同时进入选举
				rf.ConvertToCandidate()
				go rf.sendAllRequestVote()
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

// AppendEntriesReply 回退优化
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// SendAllAppendEntries 由Leader向其他所有节点调用来复制日志条目;也用作heartbeat
func (rf *Raft) SendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("Term %v Leader %v Send all AppendEntries Log : %v", rf.currentTerm, rf.me, rf.logs)
	for server := range rf.peers {
		if server != rf.me && rf.role == Leader {
			// 向follower发送nextIndex最新log日志
			// 如果无需更新 则发送心跳即可。
			// 为了效率 可以一次发送多份
			// 这一段要在锁内处理，防止越界。
			nxtId := rf.nextIndex[server]
			firstLog := rf.getFirstLog()
			// 此时发送AppendEntries信号，让节点复制日志
			// 否则则直接发送快照文件，让子节点复制
			if nxtId > firstLog.Index {
				nxtId -= firstLog.Index
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
					reply := &AppendEntriesReply{}
					rf.SendAppendEntries(id, args, reply)
				}(server, args)
			} else {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: firstLog.Index,
					LastIncludedTerm:  firstLog.Term,
					Data:              rf.persister.ReadSnapshot(),
				}

				go func(id int, args *InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.SendInstallSnapshotRpc(id, args, reply)
				}(server, args)
			}
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
	defer rf.persist()
	// 阻止过时RPC
	if args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
	}

	if rf.role != Leader {
		return
	}
	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	// 优化
	// 在收到一个冲突响应后，领导者首先应该搜索其日志中任期为 conflictTerm 的条目。
	// 如果领导者在其日志中找到此任期的一个条目，则应该设置 nextIndex 为其日志中此任期的最后一个条目的索引的下一个。
	// 如果领导者没有找到此任期的条目，则应该设置 nextIndex = conflictIndex。
	if !reply.Success {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[id] = reply.ConflictIndex
		} else {
			// 2D更新，注意日志的下标偏移
			snapLastIndex := rf.getFirstLog().Index
			flag := true
			for j := len(rf.logs) - 1; j >= 0; j-- {
				if rf.logs[j].Term == reply.ConflictTerm {
					rf.nextIndex[id] = j + 1 + snapLastIndex
					flag = false
					break
				} else if rf.logs[j].Term < reply.ConflictTerm {
					break
				}
			}
			if flag {
				rf.nextIndex[id] = reply.ConflictIndex
			}
		}
	} else {
		rf.nextIndex[id] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[id])
		rf.matchIndex[id] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[id])
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
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,Term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	defer DPrintf("{Node %v}Logs %v", rf.me, rf.logs)
	reply.Term = rf.currentTerm
	// 老Leader重连后Follower不接受旧信号
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.role == Candidate) {
		rf.ConvertToFollower(args.Term)
	}
	// 发送心跳重置计时器
	rf.HeartBeatChan <- struct{}{}
	// 如果追随者的日志中没有 preLogIndex，它应该返回 conflictIndex = len(log) 和 conflictTerm = None。

	if args.PrevLogIndex >= rf.getLogLen() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLogLen()
		return
	}
	// 2D更新，注意日志的下标偏移
	snapLastIndex := rf.getFirstLog().Index
	lastLog := rf.logs[args.PrevLogIndex-snapLastIndex]
	// 最后的日志对不上 因此需要让Leader对该节点的nextIndex - 1。
	// 优化
	// 如果追随者的日志中有 preLogIndex，但是任期不匹配，它应该返回 conflictTerm = log[preLogIndex].Term，
	// 然后在它的日志中搜索任期等于 conflictTerm 的第一个条目索引。
	if args.PrevLogTerm != lastLog.Term {
		reply.ConflictTerm = lastLog.Term
		for j := args.PrevLogIndex; j >= snapLastIndex; j-- {
			if rf.logs[j-snapLastIndex].Term != lastLog.Term {
				reply.ConflictIndex = j + 1
				break
			}
		}
		return
	}
	reply.Success = true
	// 在PrevLogIndex处开始复制一份日志
	// 这里要循环判断冲突再复制 不然可能由于滞后性删除了logs
	for idx := 0; idx < len(args.Entries); idx++ {
		curIdx := idx + args.PrevLogIndex + 1 - snapLastIndex
		if curIdx >= len(rf.logs) || rf.logs[curIdx].Term != args.Entries[idx].Term {
			//if curIdx < len(rf.logs) {
			//	defer DPrintf("Conflict logs %v %v %v", rf.logs[curIdx], args.Entries[idx], rf.logs[curIdx].Term != args.Entries[idx].Term)
			//}
			//defer DPrintf("logs Replace when curId %v index %v logsLen %v ", curIdx, idx, len(rf.logs))
			rf.logs = append(rf.logs[:curIdx], args.Entries[idx:]...)
			//defer DPrintf("AfterLogs %v", rf.logs)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.getLogLen(), args.LeaderCommit)
	}
}

func (rf *Raft) sendAllRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLog := rf.getLastLog()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	defer DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)

	for i := range rf.peers {
		if i != rf.me && rf.role == Candidate {
			go func(id int) {
				ret := &RequestVoteReply{}
				rf.sendRequestVote(id, args, ret)
			}(i)
		}
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.trimIndex(index)
	// 保存状态和日志
	rf.persister.Save(rf.EncoderState(), snapshot)
}

func (rf *Raft) trimIndex(index int) {
	snapShotIndex := rf.getFirstLog().Index
	if snapShotIndex >= index {
		return
	}
	// rf.logs[0]保留快照的lastLog
	// 释放大切片内存
	rf.logs = append([]LogEntries{}, rf.logs[index-snapShotIndex:]...)
	rf.logs[0].Command = nil
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s Term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // Term of LastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at Offset
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SendInstallSnapshotRpc(id int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[id].Call("Raft.InstallSnapshotHandler", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
	}
	snapshotIndex := rf.getFirstLog().Index
	// RPC任期不匹配、或者退位、或者快照下标对不上直接返回即可。
	if rf.currentTerm != args.Term || rf.role != Leader || args.LastIncludedIndex != snapshotIndex {
		return
	}
	rf.nextIndex[id] = max(rf.nextIndex[id], args.LastIncludedIndex+1)
	rf.matchIndex[id] = max(rf.matchIndex[id], args.LastIncludedIndex)

	rf.persister.Save(rf.EncoderState(), args.Data)
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,Term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.role == Candidate) {
		rf.ConvertToFollower(args.Term)
	}
	rf.HeartBeatChan <- struct{}{}
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	// 全盘接受快照文件
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	if rf.getLastLog().Index <= args.LastIncludedIndex {
		rf.logs = []LogEntries{{
			Command: nil,
			Term:    args.LastIncludedTerm,
			Index:   args.LastIncludedIndex,
		}}
	} else {
		snapIndex := rf.getFirstLog().Index
		newLogs := make([]LogEntries, rf.getLastLog().Index-args.LastIncludedIndex+1)
		copy(newLogs, rf.logs[args.LastIncludedIndex-snapIndex:])
		rf.logs = newLogs
		rf.logs[0].Command = nil
		rf.logs[0].Term = args.LastIncludedTerm
		rf.logs[0].Index = args.LastIncludedIndex
	}
	rf.persister.Save(rf.EncoderState(), args.Data)
	go func() {
		rf.applyChan <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) getLastLog() LogEntries {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() LogEntries {
	return rf.logs[0]
}

func (rf *Raft) getLogLen() int {
	return rf.getLastLog().Index + 1
}
