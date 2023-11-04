package kvraft

import (
	"labs-6.824/src/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId int64
	// 实现线性一致性,将读写作为顺序命令输入，严格按照顺序
	commandId int
	// 缓存一下上次Leader的消息，不用每次都遍历
	LeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	// You'll have to add code here.
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.commandId++
	args := &GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		CommandId: ck.commandId,
	}
	// 轮询访问
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.Get", args, reply)
		if ok && reply.Err == OK {
			return reply.Value
		} else {
			// 当前客户端网络异常，向其他客户端发送
			// 超时等待重发即可
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
		}
		if ck.LeaderId%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandId++
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		CommandId: ck.commandId,
	}
	// 轮询访问
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			return
		} else {
			// 当前客户端网络异常，向其他客户端发送
			// 超时等待重发即可
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
		}
		if ck.LeaderId%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 复用一下Get操作的Arg和Reply,key为空字符串表示单纯查询该节点是否为Leader
func (ck *Clerk) findLeaderId() {
	for serverId := range ck.servers {
		reply := &GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &GetArgs{}, reply)
		if ok && reply.Err == OK {
			ck.LeaderId = serverId
			return
		}
	}
}
