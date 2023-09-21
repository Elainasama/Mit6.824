package raft

// RoleType 记录节点的三种状态
type RoleType int

const noVoted = -1
const (
	Leader RoleType = iota
	Candidate
	Follower
)
