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

const Debug = 1

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
	Role OpType

	// kv
	Key       string
	Value     string
	KvType    string
	ClerkId   int64
	CommandId int
	// UpdateConfiguration
	Config shardctrler.Config
	// MoveShard
	Shard int
	Data  []byte
	Num   int
}

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
	kvMemory      [shardctrler.NShards]map[string]string
	Status        [shardctrler.NShards]ShardStatus
	Session       [shardctrler.NShards]deadlock.Map
	mck           *shardctrler.Clerk
	PrevConfig    shardctrler.Config
	CurrentConfig shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("kvServer receive Get Op args:%v reply:%v", args, reply)

	// 检查当前请求的分片是否属于当前组
	if !kv.isShardInGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		fmt.Println(kv.me, kv.Status)
		return
	}

	// 已经存放旧结果
	res := kv.getSessionResult(args.Shard, args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.Err = OK
		reply.Value = res.Value
		return
	}

	go kv.rf.Start(Op{
		Role:      KvOp,
		Shard:     args.Shard,
		Key:       args.Key,
		KvType:    Get,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	})

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		// 检查当前请求的分片是否属于当前组
		if !kv.isShardInGroup(args.Shard) {
			reply.Err = ErrWrongGroup
			return
		}
		res = kv.getSessionResult(args.Shard, args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = res.Err
			reply.Value = res.Value
			return
		}
		// 这里不要Sleep太久自己卡自己,否则过不了速度测试
		time.Sleep(QueryTime)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	defer DPrintf("kvServer receive PutAppend Op args:%v reply:%v", args, reply)
	// 检查当前请求的分片是否属于当前组
	if !kv.isShardInGroup(args.Shard) {
		reply.Err = ErrWrongGroup
		fmt.Println(kv.me, kv.Status)
		return
	}

	// 已经存放旧结果
	res := kv.getSessionResult(args.Shard, args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.Err = OK
		return
	}

	go kv.rf.Start(Op{
		Role:      KvOp,
		Shard:     args.Shard,
		Key:       args.Key,
		Value:     args.Value,
		KvType:    args.Op,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	})

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}

		// 检查当前请求的分片是否属于当前组
		if !kv.isShardInGroup(args.Shard) {
			reply.Err = ErrWrongGroup
			return
		}

		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = res.Err
			return
		}
		// 这里不要Sleep太久自己卡自己，否则过不了速度测试
		time.Sleep(QueryTime)
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
	labgob.Register(Op{})

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
	kv.Session = [shardctrler.NShards]deadlock.Map{}
	kv.Status = [shardctrler.NShards]ShardStatus{}
	for i := range kv.Status {
		kv.Status[i] = Delete
	}
	for i := range kv.kvMemory {
		kv.kvMemory[i] = make(map[string]string)
		kv.Session[i] = deadlock.Map{}
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersist(kv.rf.Persister.ReadSnapshot())

	go kv.Run()
	go kv.ShardCtrlerPullConfig()
	go kv.MoveShardWorker()
	go kv.DeleteShardWorker()
	return kv
}

// 持久化 存储快照文件
func (kv *ShardKV) persist(lastApplyIndex int) {
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	mapCopy := [len(kv.Session)]map[interface{}]sessionResult{}
	for shard := range mapCopy {
		mapCopy[shard] = make(map[interface{}]sessionResult)
		kv.Session[shard].Range(func(key, value any) bool {
			mapCopy[shard][key] = value.(sessionResult)
			return true
		})
	}
	if e.Encode(mapCopy) != nil || e.Encode(kv.kvMemory) != nil || e.Encode(kv.Status) != nil || e.Encode(kv.PrevConfig) != nil || e.Encode(kv.CurrentConfig) != nil {
		log.Fatal("Errors occur when kv Encoder")
	}
	data := w.Bytes()
	// 由于raft是异步提交 这里kv手动维护ApplyIndex
	kv.rf.Snapshot(lastApplyIndex, data)
}

func (kv *ShardKV) readPersist(data []byte) {
	if kv.maxraftstate == -1 || data == nil || len(data) < 1 {
		// bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var kvMemory [len(kv.kvMemory)]map[string]string
	var mapCopy [len(kv.Session)]map[interface{}]sessionResult
	var status [len(kv.kvMemory)]ShardStatus
	var preConfig shardctrler.Config
	var curConfig shardctrler.Config
	if d.Decode(&mapCopy) != nil || d.Decode(&kvMemory) != nil || d.Decode(&status) != nil || d.Decode(&preConfig) != nil || d.Decode(&curConfig) != nil {
		log.Fatal("Errors occur when kv Decoder")
	} else {
		kv.kvMemory = kvMemory
		for shard := range mapCopy {
			for k, v := range mapCopy[shard] {
				kv.Session[shard].Store(k, v)
			}
		}
		kv.Status = status
		kv.PrevConfig = preConfig
		kv.CurrentConfig = curConfig
	}
}

func (kv *ShardKV) Run() {
	for !kv.killed() {
		select {
		case applyEntity := <-kv.applyCh:
			op, _ := applyEntity.Command.(Op)
			switch op.Role {
			case KvOp:
				// 这里需要保证线性执行 不要并发
				if applyEntity.SnapshotValid {
					kv.readPersist(applyEntity.Snapshot)
				} else {
					kv.doApplyWork(applyEntity.CommandIndex, op)
				}
			case MoveShard:
				kv.doMoveShardWork(applyEntity.CommandIndex, op)
			case UpdateConfiguration:
				kv.doUpdateConfigurationWork(applyEntity.CommandIndex, op)
			case DeleteShard:
				kv.doDeleteShardWork(applyEntity.CommandIndex, op)
			}

		}
	}
}

func (kv *ShardKV) doApplyWork(lastApplyIndex int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)

	term, isLeader := kv.rf.GetState()

	defer DPrintf("Term %v node %v status %v receive ApplyLog:%v", term, kv.me, isLeader, op)
	defer DPrintf("session:%v,Memory:%v", kv.getSessionResult(op.Shard, op.ClerkId), kv.kvMemory)

	// 检查当前请求的分片是否属于当前组
	// 并判断该分片是否在迁移或待删除中，此时不可服务
	// 日志更新可能比迁移来得更快
	// kv.CurrentConfig.Shards[op.Shard] != kv.gid
	if kv.Status[op.Shard] != Serving || kv.CurrentConfig.Shards[op.Shard] != kv.gid {
		return
	}

	// 已有结果，直接返回即可。
	res := kv.getSessionResult(op.Shard, op.ClerkId)
	if res.LastCommandId+1 != op.CommandId {
		return
	}
	// map的取值返回两个值，不应该直接调用函数QAQ
	switch op.KvType {
	case Get:
		kv.Session[op.Shard].Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Value:         kv.kvMemory[op.Shard][op.Key],
			Err:           OK,
		})
	case Put:
		kv.kvMemory[op.Shard][op.Key] = op.Value
		kv.Session[op.Shard].Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Value:         op.Value,
			Err:           OK,
		})
	case Append:
		kv.kvMemory[op.Shard][op.Key] += op.Value
		kv.Session[op.Shard].Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Value:         kv.kvMemory[op.Shard][op.Key],
			Err:           OK,
		})
	}
}

func (kv *ShardKV) getSessionResult(shard int, clerkId int64) sessionResult {
	res, exist := kv.Session[shard].Load(clerkId)
	if !exist {
		return sessionResult{}
	}
	return res.(sessionResult)
}

// ShardCtrlerPullConfig 定期拉取分片控制器的最新配置信息
func (kv *ShardKV) ShardCtrlerPullConfig() {
	for !kv.killed() {
		if kv.isLeader() && kv.checkUpdateConfig() {
			kv.mu.RLock()
			configNum := kv.CurrentConfig.Num
			kv.mu.RUnlock()

			conf := kv.mck.Query(configNum + 1)
			if conf.Num == configNum+1 {
				go kv.rf.Start(Op{
					Role:   UpdateConfiguration,
					Config: conf,
				})
			}

		}
		time.Sleep(CheckTime)
	}
}

// 此时如果存在分片拉取行动，阻止拉取新日志
func (kv *ShardKV) checkUpdateConfig() bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	for _, status := range kv.Status {
		// 处于过渡态时不允许拉取新日志
		if status == Pulling || status == Pushing || status == WaitingPushing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) doUpdateConfigurationWork(lastApplyIndex int, op Op) {
	// 按顺序拉取
	conf := op.Config
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)

	// 只更新最新的日志
	if conf.Num != kv.CurrentConfig.Num+1 {
		return
	}

	// 更新新配置
	kv.PrevConfig = kv.CurrentConfig
	kv.CurrentConfig = conf

	for i := range conf.Shards {
		// 新增分片在当前节点下，暂时不服务，去拉取旧节点要分片数据
		// 初始化为0不计入迁移中
		if kv.PrevConfig.Shards[i] == 0 && kv.CurrentConfig.Shards[i] == kv.gid {
			kv.Status[i] = Serving
		}
		// 去某处拉取分片
		if kv.PrevConfig.Shards[i] != kv.gid && kv.PrevConfig.Shards[i] != 0 && kv.CurrentConfig.Shards[i] == kv.gid {
			kv.Status[i] = Pulling
		}
		// 该分片被拉取
		if kv.PrevConfig.Shards[i] == kv.gid && kv.CurrentConfig.Shards[i] != kv.gid && kv.CurrentConfig.Shards[i] != 0 {
			kv.Status[i] = WaitingPushing
		}
	}
}

func (kv *ShardKV) isShardInGroup(shard int) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.Status[shard] == Serving || kv.Status[shard] == WaitingPushing
}

func (kv *ShardKV) SendMoveShardRpc(group []string, args *ShardArgs) {
	for _, server := range group {
		reply := &ShardReply{}
		ok := kv.make_end(server).Call("ShardKV.MoveShardHandler", args, reply)
		if ok && reply.Err == OK {
			// 想raft提交分片信息，等待分片转移
			go kv.rf.Start(Op{
				Role:  MoveShard,
				Shard: args.Shard,
				Data:  reply.Data,
				Num:   args.ConfigNum,
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
	if kv.CurrentConfig.Num != args.ConfigNum || kv.Status[args.Shard] != WaitingPushing {
		return
	}
	reply.Err = OK
	reply.Data = kv.StoreShardData(args.Shard)
	// 此时转换为Push，该分片不可再更新
	kv.Status[args.Shard] = Pushing
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
		time.Sleep(CheckTime)
	}

}

func (kv *ShardKV) StoreShardData(shard int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	mapCopy := map[interface{}]sessionResult{}
	kv.Session[shard].Range(func(key, value any) bool {
		mapCopy[key] = value.(sessionResult)
		return true
	})

	if e.Encode(mapCopy) != nil || e.Encode(kv.kvMemory[shard]) != nil {
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
	var mapCopy map[interface{}]sessionResult
	if d.Decode(&mapCopy) != nil || d.Decode(&kvMemory) != nil {
		log.Fatal("Errors occur when kv Decoder")
	} else {
		kv.kvMemory[shard] = kvMemory
		for k, v := range mapCopy {
			kv.Session[shard].Store(k, v)
		}
	}
}

func (kv *ShardKV) doMoveShardWork(lastApplyIndex int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)
	// 不是最新配置的分片不接受
	if op.Num != kv.CurrentConfig.Num || kv.Status[op.Shard] != Pulling {
		return
	}

	kv.LoadShardData(op.Shard, op.Data)
	kv.Status[op.Shard] = Serving
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) doDeleteShardWork(lastApplyIndex int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist(lastApplyIndex)
	if kv.CurrentConfig.Num != op.Num || kv.Status[op.Shard] != Pushing {
		return
	}
	kv.kvMemory[op.Shard] = make(map[string]string)
	kv.Session[op.Shard] = deadlock.Map{}
	kv.Status[op.Shard] = Delete
	return
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
		time.Sleep(CheckTime)
	}

}

func (kv *ShardKV) SendDeleteShardRpc(group []string, args *ShardArgs) {
	for _, server := range group {
		reply := &ShardReply{}
		ok := kv.make_end(server).Call("ShardKV.DeleteShardHandler", args, reply)
		if ok && reply.Err == OK {
			// 发送删除分片的消息，完成分片转移
			go kv.rf.Start(Op{
				Role:  DeleteShard,
				Shard: args.Shard,
				Num:   args.ConfigNum,
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
