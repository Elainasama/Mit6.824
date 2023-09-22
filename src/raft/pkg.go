package raft

import "time"

// RoleType 记录节点的三种状态
type RoleType int

// chan缓冲区长度
var chanLen = 10

// 选举超时时间为1s
var electionTimeout = 1 * time.Second

// 心跳超时时间为200ms
var appendEntriesTimeout = 200 * time.Millisecond

// HeartBeatInterval 心跳间隔 每1秒发送10次
var HeartBeatInterval = 100 * time.Millisecond

const noVoted = -1
const (
	Leader RoleType = iota
	Candidate
	Follower
)
