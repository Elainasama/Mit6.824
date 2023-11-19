package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment Gid time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

type OpType int

const (
	KvOp OpType = iota
	UpdateConfiguration
	MoveShard
	DeleteShard
	Nop
)

type ShardStatus int

const (
	Serving ShardStatus = iota
	Pulling
	Pushing
	Delete
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Shard     int
	ClerkId   int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Shard     int
	ClerkId   int64
	CommandId int
}

type GetReply struct {
	Err   Err
	Value string
}

type SessionResult struct {
	Err       Err
	Value     string
	SessionId int64
}

const ChannelLen = 0

const TimeOut = 200 * time.Millisecond
const QueryTime = 20 * time.Millisecond

// CheckTime 定期的分片拉取时间
const CheckTime = 100 * time.Millisecond

const MoveShardTime = 30 * time.Millisecond

const DeleteShardTime = 50 * time.Millisecond

const NopTime = 60 * time.Millisecond

// MoveShardTimeOut 分片迁移超时时间
const MoveShardTimeOut = 400 * time.Millisecond

type ShardArgs struct {
	Gid       int
	Shard     int
	ConfigNum int
}
type ShardReply struct {
	// for Move
	Data      []byte
	Err       Err
	ConfigNum int
}
