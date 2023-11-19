package shardkv

import (
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"labs-6.824/src/labrpc"
	"labs-6.824/src/shardctrler"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "labs-6.824/src/raft"
import "labs-6.824/src/labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	Role OpType
	Op   interface{}
}

type KVOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// kv
	Key       string
	Value     string
	KvType    string
	ClerkId   int64
	CommandId int
	Num       int
	Shard     int
	SessionId int64
}

type UpdateConfigOp struct {
	// UpdateConfiguration
	Config shardctrler.Config
}
type MoveShardOp struct {
	// MoveShard
	Shard int
	Data  []byte
	Num   int
}

type DeleteShardOp struct {
	// MoveShard
	Shard int
	Num   int
}

// ShardKV
// Challenge1不允许在Seesion存储答案，不然会爆内存。
// 所以还是用channel传输吧，可以多次查询。
type ShardKV struct {
	mu           deadlock.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32
	// 按照shard分片去划分map
	kvMemory [shardctrler.NShards]map[string]string
	Status   [shardctrler.NShards]ShardStatus
	// Sync.Map太慢了 复制的时候很麻烦 效率比不上读锁+Map
	Session          [shardctrler.NShards]map[int64]chan SessionResult
	LastCommandIndex [shardctrler.NShards]map[int64]int
	mck              *shardctrler.Clerk
	PrevConfig       shardctrler.Config
	CurrentConfig    shardctrler.Config
	// 多次重启会导致快照覆盖有问题 要判下spotIndex
	LastSpotIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("kvServer receive Get Op args:%v reply:%v", args, reply)

	kv.mu.RLock()
	// 检查当前请求的分片是否属于当前组
	if !kv.isShardInGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		//fmt.Println(kv.me, kv.Status)
		kv.mu.RUnlock()
		return
	}

	sessionId := nrand()
	op := Command{
		Role: KvOp,
		Op: KVOp{
			Shard:     args.Shard,
			Key:       args.Key,
			KvType:    Get,
			Num:       kv.CurrentConfig.Num,
			ClerkId:   args.ClerkId,
			CommandId: args.CommandId,
			SessionId: sessionId,
		},
	}
	kv.mu.RUnlock()

	kv.rf.Start(op)

	ch := kv.makeSessionChannel(args.Shard, args.ClerkId)
	QueryTimeOut := time.After(TimeOut)
	for {
		select {
		case res := <-ch:
			if res.SessionId != sessionId {
				continue
			}
			reply.Err = res.Err
			reply.Value = res.Value
			return
		case <-QueryTimeOut:
			reply.Err = ErrTimeOut
			return
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("kvServer receive PutAppend Op args:%v reply:%v", args, reply)

	kv.mu.RLock()
	// 检查当前请求的分片是否属于当前组
	if !kv.isShardInGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		//fmt.Println(kv.me, kv.Status)
		kv.mu.RUnlock()
		return
	}
	sessionId := nrand()
	op := Command{
		Role: KvOp,
		Op: KVOp{
			Shard:     args.Shard,
			Key:       args.Key,
			Value:     args.Value,
			KvType:    args.Op,
			Num:       kv.CurrentConfig.Num,
			ClerkId:   args.ClerkId,
			CommandId: args.CommandId,
			SessionId: sessionId,
		},
	}
	kv.mu.RUnlock()

	kv.rf.Start(op)

	ch := kv.makeSessionChannel(args.Shard, args.ClerkId)
	QueryTimeOut := time.After(TimeOut)
	for {
		select {
		case res := <-ch:
			if res.SessionId != sessionId {
				continue
			}
			reply.Err = res.Err
			return
		case <-QueryTimeOut:
			reply.Err = ErrTimeOut
			return
		}
	}
}

func (kv *ShardKV) makeSessionChannel(shard int, clerkId int64) chan SessionResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if channel, exist := kv.Session[shard][clerkId]; exist {
		return channel
	} else {
		kv.Session[shard][clerkId] = make(chan SessionResult, ChannelLen)
		return kv.Session[shard][clerkId]
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output Gid this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name Gid a
// CurrentConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(KVOp{})
	labgob.Register(MoveShardOp{})
	labgob.Register(DeleteShardOp{})
	labgob.Register(UpdateConfigOp{})
	labgob.Register(SessionResult{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.kvMemory = [shardctrler.NShards]map[string]string{}
	kv.Session = [shardctrler.NShards]map[int64]chan SessionResult{}
	kv.Status = [shardctrler.NShards]ShardStatus{}
	kv.LastCommandIndex = [shardctrler.NShards]map[int64]int{}
	// 初始化
	for i := range kv.kvMemory {
		kv.Status[i] = Delete
		kv.kvMemory[i] = make(map[string]string)
		kv.Session[i] = make(map[int64]chan SessionResult)
		kv.LastCommandIndex[i] = make(map[int64]int)
	}
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(kv.rf.Persister.ReadSnapshot(), 0)

	go kv.Run()
	go kv.ShardCtrlerPullConfig()
	go kv.MoveShardWorker()
	go kv.DeleteShardWorker()
	// 保证Leader准时提交新日志，消除活锁
	// 对于两个集群crash后重启，回到了shardState存在PULL但不存在PUSH的中间态，显然现在两者都不能主动地更新conf。
	go kv.NopWorker()
	return kv
}

// 持久化 存储快照文件
func (kv *ShardKV) persist(lastApplyIndex int) {
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.LastSpotIndex = lastApplyIndex
	if e.Encode(kv.kvMemory) != nil || e.Encode(kv.Status) != nil || e.Encode(kv.PrevConfig) != nil || e.Encode(kv.CurrentConfig) != nil || e.Encode(kv.LastCommandIndex) != nil || e.Encode(kv.LastSpotIndex) != nil {
		log.Fatal("Errors occur when kv Encoder")
	}
	data := w.Bytes()
	// 由于raft是异步提交 这里kv手动维护ApplyIndex
	kv.rf.Snapshot(lastApplyIndex, data)

}

func (kv *ShardKV) readPersist(data []byte, lastSpotIndex int) {
	if kv.maxraftstate == -1 || data == nil || len(data) < 1 {
		// bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.LastSpotIndex > lastSpotIndex {
		fmt.Println("Well Check!!!")
		return
	}
	var kvMemory [len(kv.kvMemory)]map[string]string
	var status [len(kv.kvMemory)]ShardStatus
	var preConfig shardctrler.Config
	var curConfig shardctrler.Config
	var LastCommandIndex [len(kv.Session)]map[int64]int

	if d.Decode(&kvMemory) != nil || d.Decode(&status) != nil || d.Decode(&preConfig) != nil || d.Decode(&curConfig) != nil || d.Decode(&LastCommandIndex) != nil || d.Decode(&lastSpotIndex) != nil {
		log.Fatal("Errors occur when kv Decoder")
	} else {
		kv.kvMemory = kvMemory
		kv.Status = status
		kv.PrevConfig = preConfig
		kv.CurrentConfig = curConfig
		kv.LastCommandIndex = LastCommandIndex
		kv.LastSpotIndex = lastSpotIndex
		fmt.Printf("%v Restart!!!\n", kv.me)
		fmt.Println(kv.Status, kv.LastCommandIndex, kv.kvMemory)
	}
}

func (kv *ShardKV) Run() {
	for !kv.killed() {
		select {
		case applyEntity := <-kv.applyCh:
			cmd, _ := applyEntity.Command.(Command)

			switch cmd.Role {
			case KvOp:
				// 这里需要保证线性执行 不要并发
				if applyEntity.SnapshotValid {
					kv.readPersist(applyEntity.Snapshot, applyEntity.SnapshotIndex)
				} else {
					op, _ := cmd.Op.(KVOp)
					kv.doApplyWork(applyEntity.CommandIndex, op)
				}
			case MoveShard:
				op, _ := cmd.Op.(MoveShardOp)
				kv.doMoveShardWork(applyEntity.CommandIndex, op)
			case UpdateConfiguration:
				op, _ := cmd.Op.(UpdateConfigOp)
				kv.doUpdateConfigurationWork(applyEntity.CommandIndex, op)
			case DeleteShard:
				op, _ := cmd.Op.(DeleteShardOp)
				kv.doDeleteShardWork(applyEntity.CommandIndex, op)
			case Nop:
				continue

			}
		case <-time.After(QueryTime):
			continue
		}
	}
}

func (kv *ShardKV) doApplyWork(lastApplyIndex int, op KVOp) {
	term, isLeader := kv.rf.GetState()
	// 又犯了同样的蠢，apply部分是所有节点都可以使用的。
	//if !isLeader {
	//	kv.sendStructWithTimeout(kv.Session[op.Shard][op.ClerkId], SessionResult{
	//		Err:       ErrWrongLeader,
	//		SessionId: op.SessionId,
	//	})
	//	return
	//}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)

	defer DPrintf("Term %v node %v status %v receive ApplyLog:%v", term, kv.me, isLeader, op)
	//defer DPrintf("session:%v,Memory:%v", kv.Session[op.Shard][op.ClerkId], kv.kvMemory)

	// 检查当前请求的分片是否属于当前组
	// 并判断该分片是否在迁移或待删除中，此时不可服务
	// 日志更新可能比迁移来得更快
	// kv.CurrentConfig.Shards[op.Shard] != kv.gid
	if op.Num != kv.CurrentConfig.Num || !kv.isShardInGroup(op.Shard) {
		kv.sendStructWithTimeout(kv.Session[op.Shard][op.ClerkId], SessionResult{
			Err:       ErrWrongGroup,
			SessionId: op.SessionId,
		})
		return
	}

	// 不可重复进行操作Put和Append
	lastCmdIdx := kv.LastCommandIndex[op.Shard][op.ClerkId]
	// 不能出现不一致现象
	if lastCmdIdx+1 < op.CommandId {
		log.Fatalf("%v %v %v \n", lastCmdIdx, op.CommandId, op)
		return
	}
	if lastCmdIdx >= op.CommandId && op.KvType != Get {
		kv.sendStructWithTimeout(kv.Session[op.Shard][op.ClerkId], SessionResult{
			Err:       OK,
			SessionId: op.SessionId,
		})
		return
	}
	if op.CommandId > kv.LastCommandIndex[op.Shard][op.ClerkId] {
		kv.LastCommandIndex[op.Shard][op.ClerkId] = op.CommandId
	}

	// map的取值返回两个值，不应该直接调用函数QAQ
	switch op.KvType {
	case Get:
		kv.sendStructWithTimeout(kv.Session[op.Shard][op.ClerkId], SessionResult{
			Value:     kv.kvMemory[op.Shard][op.Key],
			Err:       OK,
			SessionId: op.SessionId,
		})
	case Put:
		kv.kvMemory[op.Shard][op.Key] = op.Value
		kv.sendStructWithTimeout(kv.Session[op.Shard][op.ClerkId], SessionResult{
			Value:     op.Value,
			Err:       OK,
			SessionId: op.SessionId,
		})
	case Append:
		kv.kvMemory[op.Shard][op.Key] += op.Value
		kv.sendStructWithTimeout(kv.Session[op.Shard][op.ClerkId], SessionResult{
			Value:     kv.kvMemory[op.Shard][op.Key],
			Err:       OK,
			SessionId: op.SessionId,
		})
	}
}

// ShardCtrlerPullConfig 定期拉取分片控制器的最新配置信息
func (kv *ShardKV) ShardCtrlerPullConfig() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.RLock()
			check := kv.checkUpdateConfig()
			configNum := kv.CurrentConfig.Num
			kv.mu.RUnlock()

			if check {
				conf := kv.mck.Query(configNum + 1)
				if conf.Num == configNum+1 {
					kv.rf.Start(Command{
						Role: UpdateConfiguration,
						Op:   UpdateConfigOp{Config: conf},
					})
				}
			}

		}
		time.Sleep(CheckTime)
	}
}

// 此时如果存在分片拉取行动，阻止拉取新日志
func (kv *ShardKV) checkUpdateConfig() bool {
	for _, status := range kv.Status {
		// 处于过渡态时不允许拉取新日志
		if status == Pulling || status == Pushing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) doUpdateConfigurationWork(lastApplyIndex int, op UpdateConfigOp) {
	// 按顺序拉取
	conf := op.Config
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)
	// 只更新最新的日志
	if conf.Num != kv.CurrentConfig.Num+1 || !kv.checkUpdateConfig() {
		return
	}
	defer DPrintf("node %v Update Config %v get Status %v", kv.me, conf, kv.Status)
	// 更新新配置
	kv.PrevConfig = kv.CurrentConfig
	kv.CurrentConfig = conf

	for i := range conf.Shards {
		// 新增分片在当前节点下，暂时不服务，去拉取旧节点要分片数据
		// 初始化为0不计入迁移中
		// 去某处拉取分片
		if kv.PrevConfig.Shards[i] != kv.gid && kv.CurrentConfig.Shards[i] == kv.gid {
			if kv.PrevConfig.Shards[i] == 0 {
				kv.Status[i] = Serving
			} else {
				kv.Status[i] = Pulling
			}

		}
		// 该分片被拉取
		if kv.PrevConfig.Shards[i] == kv.gid && kv.CurrentConfig.Shards[i] != kv.gid {
			if kv.CurrentConfig.Shards[i] == 0 {
				kv.Status[i] = Delete
			} else {
				kv.Status[i] = Pushing
			}
		}
	}
}

func (kv *ShardKV) isShardInGroup(shard int) bool {
	return kv.Status[shard] == Serving && kv.CurrentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) SendMoveShardRpc(group []string, args *ShardArgs) {
	for _, server := range group {
		reply := &ShardReply{}
		ok := kv.make_end(server).Call("ShardKV.MoveShardHandler", args, reply)
		if ok && reply.Err == OK {
			// 想raft提交分片信息，等待分片转移
			kv.rf.Start(Command{
				Role: MoveShard,
				Op: MoveShardOp{
					Shard: args.Shard,
					Data:  reply.Data,
					Num:   args.ConfigNum,
				},
			})
			return
		}
	}
}

func (kv *ShardKV) MoveShardHandler(args *ShardArgs, reply *ShardReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 此时这部分还未更新 先等待
	if kv.CurrentConfig.Num != args.ConfigNum || kv.Status[args.Shard] != Pushing {
		return
	}
	reply.Err = OK
	reply.Data = kv.StoreShardData(args.Shard)
	// 此时转换为Push，该分片不可再更新
	//kv.Status[args.Shard] = Pushing
}

func (kv *ShardKV) MoveShardWorker() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.RLock()
			wg := sync.WaitGroup{}
			for i := range kv.Status {
				if kv.Status[i] == Pulling {
					// 向谁拉取
					wg.Add(1)
					preGid := kv.PrevConfig.Shards[i]
					args := &ShardArgs{
						Gid:       kv.gid,
						Shard:     i,
						ConfigNum: kv.CurrentConfig.Num,
					}
					go func() {
						kv.SendMoveShardRpc(kv.PrevConfig.Groups[preGid], args)
						wg.Done()
					}()
				}
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(MoveShardTime)
	}

}

func (kv *ShardKV) StoreShardData(shard int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.kvMemory[shard]) != nil || e.Encode(kv.LastCommandIndex[shard]) != nil {
		log.Fatal("Errors occur when kv Encoder")
	}
	return w.Bytes()
}

func (kv *ShardKV) LoadShardData(shard int, data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMemory map[string]string
	var LastCommandIndex map[int64]int
	if d.Decode(&kvMemory) != nil || d.Decode(&LastCommandIndex) != nil {
		log.Fatal("Errors occur when kv Decoder")
	} else {
		kv.kvMemory[shard] = kvMemory
		kv.LastCommandIndex[shard] = LastCommandIndex
	}
}

func (kv *ShardKV) doMoveShardWork(lastApplyIndex int, op MoveShardOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)
	// 不是最新配置的分片不接受
	if op.Num != kv.CurrentConfig.Num || kv.Status[op.Shard] != Pulling {
		return
	}

	kv.LoadShardData(op.Shard, op.Data)
	kv.Status[op.Shard] = Serving
	//fmt.Println(kv.gid, kv.CurrentConfig.Num, kv.me, op.Shard)
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) doDeleteShardWork(lastApplyIndex int, op DeleteShardOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)
	if kv.CurrentConfig.Num != op.Num || kv.Status[op.Shard] != Pushing {
		return
	}
	kv.kvMemory[op.Shard] = make(map[string]string)
	//kv.Session[op.Shard] = make(map[int64]chan SessionResult)
	kv.LastCommandIndex[op.Shard] = make(map[int64]int)
	kv.Status[op.Shard] = Delete
}

func (kv *ShardKV) DeleteShardWorker() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.RLock()
			wg := sync.WaitGroup{}
			for i := range kv.Status {
				if kv.Status[i] == Pushing {
					wg.Add(1)
					// 向谁询问
					curGid := kv.CurrentConfig.Shards[i]
					args := &ShardArgs{
						Gid:       kv.gid,
						Shard:     i,
						ConfigNum: kv.CurrentConfig.Num,
					}
					go func() {
						kv.SendDeleteShardRpc(kv.CurrentConfig.Groups[curGid], args)
						wg.Done()
					}()
				}
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(DeleteShardTime)
	}

}

func (kv *ShardKV) SendDeleteShardRpc(group []string, args *ShardArgs) {
	for _, server := range group {
		reply := &ShardReply{}
		ok := kv.make_end(server).Call("ShardKV.DeleteShardHandler", args, reply)
		if ok && reply.Err == OK {
			// 发送删除分片的消息，完成分片转移
			kv.rf.Start(Command{
				Role: DeleteShard,
				Op: DeleteShardOp{
					Shard: args.Shard,
					Num:   args.ConfigNum,
				},
			})
			return
		}
	}
}

func (kv *ShardKV) DeleteShardHandler(args *ShardArgs, reply *ShardReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.CurrentConfig.Num > args.ConfigNum || kv.CurrentConfig.Num == args.ConfigNum && kv.Status[args.Shard] == Serving {
		reply.Err = OK
	}
}

func (kv *ShardKV) sendStructWithTimeout(ch chan<- SessionResult, data SessionResult) {
	if ch == nil {
		return
	}
	select {
	case ch <- data:
		// 结构体成功发送到通道
	case <-time.After(QueryTime):
		// 超时
	}
}

// NopWorker 避免活锁定期发送Nop
func (kv *ShardKV) NopWorker() {
	for !kv.killed() {
		Term, isLeader := kv.rf.GetState()
		kv.rf.Mu.RLock()
		check := Term != kv.rf.GetLastLog().Term
		kv.rf.Mu.RUnlock()
		if isLeader && check {
			kv.rf.Start(Command{
				Role: Nop,
			})
		}
		time.Sleep(NopTime)
	}
}
