package shardctrler

import (
	"github.com/sasha-s/go-deadlock"
	"labs-6.824/src/raft"
	"log"
	"sort"
	"sync/atomic"
	"time"
)
import "labs-6.824/src/labrpc"
import "sync"
import "labs-6.824/src/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32
	configs []Config // indexed by config num
	Session deadlock.Map
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your data here.
	OpType    string
	ClerkId   int64
	CommandId int
	Servers   map[int][]string // for join
	GIDs      []int            // for leave
	Shard     int              // for move
	GID       int              // for move
	Num       int              // for query
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	res := sc.getSessionResult(args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		return
	}

	sc.rf.Start(Op{
		OpType:    Join,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		Servers:   args.Servers,
	})

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		res = sc.getSessionResult(args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.WrongLeader = res.WrongLeader
			reply.Err = res.Err
			return
		}
		time.Sleep(QueryTime)
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	res := sc.getSessionResult(args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		return
	}

	sc.rf.Start(Op{
		OpType:    Leave,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		GIDs:      args.GIDs,
	})

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		res = sc.getSessionResult(args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.WrongLeader = res.WrongLeader
			reply.Err = res.Err
			return
		}
		time.Sleep(QueryTime)
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	res := sc.getSessionResult(args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		return
	}

	sc.rf.Start(Op{
		OpType:    Move,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		Shard:     args.Shard,
		GID:       args.GID,
	})

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		res = sc.getSessionResult(args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.WrongLeader = res.WrongLeader
			reply.Err = res.Err
			return
		}
		time.Sleep(QueryTime)
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	res := sc.getSessionResult(args.ClerkId)
	if res.LastCommandId == args.CommandId && res.Err == OK {
		reply.WrongLeader = res.WrongLeader
		reply.Err = res.Err
		reply.Config = res.Config
		return
	}

	sc.rf.Start(Op{
		OpType:    Query,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		Num:       args.Num,
	})

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut {
			reply.Err = ErrTimeOut
			return
		}
		res = sc.getSessionResult(args.ClerkId)
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.WrongLeader = res.WrongLeader
			reply.Err = res.Err
			reply.Config = res.Config
			return
		}
		time.Sleep(QueryTime)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardctrler tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Run() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			sc.doApplyWork(msg)
		}
	}
}

func (sc *ShardCtrler) doApplyWork(msg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := msg.Command.(Op)
	DPrintf("ShardCtrler %v receive args %v lastConfig %v", sc.me, msg, sc.getConfigByIndex(-1))
	res := sc.getSessionResult(op.ClerkId)
	if res.LastCommandId >= op.CommandId {
		return
	}

	switch op.OpType {
	case Query:
		sc.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Config:        sc.getConfigByIndex(op.Num),
			Err:           OK,
		})
	case Move:
		// go中直接赋值是直接拷贝这个结构体 非指针
		newConfig := sc.getConfigByIndex(-1)
		newConfig.Num++
		newConfig.Shards[op.Shard] = op.GID

		sc.configs = append(sc.configs, newConfig)
		sc.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Err:           OK,
		})
	case Join:
		newConfig := sc.getConfigByIndex(-1)
		newConfig.Num++
		for k, values := range op.Servers {
			newConfig.Groups[k] = append(newConfig.Groups[k], values...)
		}

		newConfig.AdjustShard()

		sc.configs = append(sc.configs, newConfig)
		sc.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Err:           OK,
		})
	case Leave:
		newConfig := sc.getConfigByIndex(-1)
		newConfig.Num++

		for _, gid := range op.GIDs {
			for i := range newConfig.Shards {
				if newConfig.Shards[i] == gid {
					newConfig.Shards[i] = 0
				}
			}
			delete(newConfig.Groups, gid)
		}

		newConfig.AdjustShard()

		sc.configs = append(sc.configs, newConfig)
		sc.Session.Store(op.ClerkId, sessionResult{
			LastCommandId: op.CommandId,
			Err:           OK,
		})
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	go sc.Run()
	return sc
}

func (sc *ShardCtrler) getSessionResult(clerkId int64) sessionResult {
	res, exist := sc.Session.Load(clerkId)
	if !exist {
		return sessionResult{}
	}
	return res.(sessionResult)
}

func (sc *ShardCtrler) getConfigByIndex(idx int) Config {
	if idx == -1 || idx >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	} else {
		return sc.configs[idx].Copy()
	}

}

func (conf *Config) AdjustShard() {
	// 建立频率map
	cnt := make(map[int]int)
	for k := range conf.Groups {
		cnt[k] = 0
	}
	for _, gid := range conf.Shards {
		if gid != 0 {
			cnt[gid]++
		}
	}

	// 这里需要保证map的遍历顺序是固定的
	var keys []int
	for key := range cnt {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	// 消除极大极小差距
	for {
		mxGid, miGid := getMaxFrequent(cnt, keys), getMinFrequent(cnt, keys)
		if cnt[mxGid]-cnt[miGid] <= 1 {
			break
		}
		for i := range conf.Shards {
			if conf.Shards[i] == mxGid {
				conf.Shards[i] = miGid
				cnt[mxGid]--
				cnt[miGid]++
				break
			}
		}
	}
	// 将剩余0的部分填入最小分组（Leave）
	for i := range conf.Shards {
		if conf.Shards[i] == 0 {
			miGid := getMinFrequent(cnt, keys)
			cnt[miGid]++
			conf.Shards[i] = miGid
		}
	}
}

func getMinFrequent(cnt map[int]int, keys []int) int {
	mi := 100000000
	miGid := 0
	for _, key := range keys {
		fre := cnt[key]
		if fre < mi {
			mi = fre
			miGid = key
		}
	}
	return miGid
}

func getMaxFrequent(cnt map[int]int, keys []int) int {
	mx := -1
	mxGid := 0
	for _, key := range keys {
		fre := cnt[key]
		if fre > mx {
			mx = fre
			mxGid = key
		}
	}
	return mxGid
}

// Copy Config实现深拷贝
func (conf Config) Copy() Config {
	newConf := Config{
		Num:    conf.Num,
		Shards: conf.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range conf.Groups {
		newConf.Groups[k] = v
	}
	return newConf
}
