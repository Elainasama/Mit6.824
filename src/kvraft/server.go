package kvraft

import (
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"labs-6.824/src/labgob"
	"labs-6.824/src/labrpc"
	"labs-6.824/src/raft"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Type      string
	ClerkId   int64
	CommandId int
}

// 每个客户端一次只会轮询发送一个消息，不处理完上一个不会发下一个
// 可以用Session简化流程
type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMemory       map[string]string
	Session        deadlock.Map
	opCh           chan Op
	lastApplyIndex int
	//lastSpotIndex  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 该请求仅查询LeaderId
	if args.Key == "" {
		reply.Err = OK
		return
	}
	defer DPrintf("kvServer receive Get Op args: %v reply: %v", args, reply)
	// 已经存放旧结果
	res := kv.getSessionResult(args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.Err = OK
		reply.Value = res.Value
		return
	}
	//
	op := Op{
		Key:       args.Key,
		Type:      "Get",
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	kv.doOpWork(op)
	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		res = kv.getSessionResult(args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = res.Err
			reply.Value = res.Value
			return
		}
		// 这里不要Sleep太久自己卡自己,否则过不了速度测试
		time.Sleep(QueryTime)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("kvServer receive PutAppend Op args:%v reply:%v", args, reply)

	// 已经存放旧结果
	res := kv.getSessionResult(args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.Err = OK
		return
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	kv.doOpWork(op)

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		res = kv.getSessionResult(args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = res.Err
			return
		}
		// 这里不要Sleep太久自己卡自己，否则过不了速度测试
		time.Sleep(QueryTime)
	}
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Run() {
	for !kv.killed() {
		select {
		case applyEntity := <-kv.applyCh:
			// 这里需要保证线性执行 不要并发
			kv.doApplyWork(applyEntity)
		}
	}
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	// 保证发送和接收一定是同步的。
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMemory = make(map[string]string)
	kv.opCh = make(chan Op, chanLen)
	kv.readPersist(persister.ReadSnapshot())
	go kv.Run()
	go kv.doSnapShotCheck()
	return kv
}

func (kv *KVServer) doSnapShotCheck() {
	for !kv.killed() {
		kv.persist()
		time.Sleep(200 * time.Millisecond)
	}
}

// 持久化 存储快照文件
func (kv *KVServer) persist() {
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	mapCopy := map[interface{}]sessionResult{}
	kv.Session.Range(func(key, value any) bool {
		mapCopy[key] = value.(sessionResult)
		return true
	})

	if e.Encode(mapCopy) != nil || e.Encode(kv.kvMemory) != nil || e.Encode(kv.lastApplyIndex) != nil {
		log.Fatal("Errors occur when kv Encoder")
	}
	data := w.Bytes()
	// 由于raft是异步提交 这里kv手动维护ApplyIndex
	kv.rf.Snapshot(kv.lastApplyIndex, data)
}

func (kv *KVServer) readPersist(data []byte) {
	if kv.maxraftstate == -1 || data == nil || len(data) < 1 {
		// bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var kvMemory map[string]string
	var mapCopy map[interface{}]sessionResult
	var applyIndex int
	if d.Decode(&mapCopy) != nil || d.Decode(&kvMemory) != nil || d.Decode(&applyIndex) != nil {
		log.Fatal("Errors occur when kv Decoder")
	} else {
		kv.kvMemory = kvMemory
		for k, v := range mapCopy {
			kv.Session.Store(k, v)
		}
		kv.lastApplyIndex = applyIndex
	}
}

// 对客户端的操作请求作出回应，启动raft并等待结果
func (kv *KVServer) doOpWork(op Op) {
	// 已有结果，直接返回即可。
	res := kv.getSessionResult(op.ClerkId)
	if res.LastCommandId >= op.CommandId {
		return
	}
	kv.rf.Start(op)
}

func (kv *KVServer) doApplyWork(msg raft.ApplyMsg) {
	// 加锁，map不能并发读写
	// 按raft顺序提交的命令居然在这里会乱序！
	//globalApplyLog.mu.Lock()
	//globalApplyLog.Map[kv.me] = append(globalApplyLog.Map[kv.me], msg)
	//fmt.Println(kv.me, globalApplyLog.Map[kv.me])
	//globalApplyLog.mu.Unlock()
	// 读取快照
	if msg.SnapshotValid {
		kv.readPersist(msg.Snapshot)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 日志提交是强制异步的，可能存在并发问题。这里不能强求线性,虽然增加缓冲区后这种情况已经不太可能发生了
	//if msg.CommandIndex != kv.lastApplyIndex+1 {
	//	if msg.CommandIndex > kv.lastApplyIndex+1 {
	//		panic("不满足线性提交")
	//	}
	//	return
	//}
	//kv.lastApplyIndex++
	op, _ := msg.Command.(Op)
	term, isLeader := kv.rf.GetState()
	// 旧的日志消息
	if msg.CommandIndex <= kv.lastApplyIndex {
		return
	}

	// 调试用
	if msg.CommandIndex != kv.lastApplyIndex+1 {
		fmt.Println(kv.me, msg.CommandIndex, kv.lastApplyIndex)
	}

	kv.lastApplyIndex++
	defer DPrintf("Term %v node %v status %v receive ApplyLog:%v", term, kv.me, isLeader, msg)
	//defer DPrintf("session:%v,Memory:%v", kv.Session.Map, kv.kvMemory)

	// 已有结果，直接返回即可。
	res := kv.getSessionResult(op.ClerkId)
	if res.LastCommandId >= op.CommandId {
		return
	}

	switch op.Type {
	case "Get":
		kv.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Value:         kv.kvMemory[op.Key],
			Err:           OK,
		})
	case "Put":
		kv.kvMemory[op.Key] = op.Value
		kv.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Value:         op.Value,
			Err:           OK,
		})
	case "Append":
		kv.kvMemory[op.Key] += op.Value
		kv.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Value:         kv.kvMemory[op.Key],
			Err:           OK,
		})
	}

}

func (kv *KVServer) genSessionId(clerkId int64, commandId int) string {
	return strconv.FormatInt(clerkId, 10) + "_" + strconv.Itoa(commandId)
}

func (kv *KVServer) decodeSessionId(sessionId string) (int64, int) {
	li := strings.Split(sessionId, "_")
	clerkId, _ := strconv.ParseInt(li[0], 10, 64)
	commandId, _ := strconv.Atoi(li[1])
	return clerkId, commandId
}

func (kv *KVServer) getSessionResult(clerkId int64) sessionResult {
	res, exist := kv.Session.Load(clerkId)
	if !exist {
		return sessionResult{}
	}
	return res.(sessionResult)
}
