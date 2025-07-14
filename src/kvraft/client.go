package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Get(key string) string {
	return ck.ExecuteCommand(&CommandArgs{Key: key, Op: OpGet})
}
func (ck *Clerk) Put(key string, value string) {
	ck.ExecuteCommand(&CommandArgs{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	ck.ExecuteCommand(&CommandArgs{Key: key, Value: value, Op: OpAppend})
}

func (ck *Clerk) ExecuteCommand(args *CommandArgs) string {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		reply := new(CommandReply)
		//当发生调用失败，错误的leader，命名执行超时
		if !ck.servers[ck.leaderId].Call("KVServer.ExecuteCommand", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId += 1
		return reply.Value
	}
}
