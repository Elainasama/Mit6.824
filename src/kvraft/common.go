package kvraft

import (
	"labs-6.824/src/raft"
	"sync"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrNetWork     = "ErrNetWork"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId   int64
	CommandId int
}

type GetReply struct {
	Err   Err
	Value string
}

type sessionResult struct {
	LastCommandId int
	Value         string
	Err           Err
}

// channel缓冲区长度
const chanLen = 100

// TimeOut 操作超时时间
const TimeOut = 1000 * time.Millisecond

// QueryTime 操作查询间隔
const QueryTime = 10 * time.Millisecond

// 看全局log用的
type syncMap struct {
	mu  sync.Mutex
	Map map[int][]raft.ApplyMsg
}

var globalApplyLog = syncMap{
	mu:  sync.Mutex{},
	Map: make(map[int][]raft.ApplyMsg),
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
