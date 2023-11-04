package raft

import "time"

// RoleType 记录节点的三种状态
// 别名类型好像很难用atomic操作
type RoleType int32

// chan缓冲区长度
var chanLen = 100

// 选举超时时间为0.3s
var electionTimeout = 300 * time.Millisecond

// 心跳超时时间为300ms
var appendEntriesTimeout = 300 * time.Millisecond

// heartBeatInterval 心跳间隔 每1秒发送10次
var heartBeatInterval = 100 * time.Millisecond

// commitInterval rf节点提交日志的间隔时间
var commitInterval = 10 * time.Millisecond

const noVoted = -1
const (
	Leader int32 = iota
	Candidate
	Follower
)

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
