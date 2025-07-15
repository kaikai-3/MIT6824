package shardctrler

// Shardctrler clerk.

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

// Clerk represents a client that interacts with the shard controller.
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int64
	clientId  int64
	commandId int64
}

// nrand generates a random 62-bit integer.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 创建一个客户端实例
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
}

// Join 向分片控制器发送jion命令
func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{Op: Join, Servers: servers}
	ck.Command(args)
}

// Leave向分片控制器发送Leave命令
func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{Op: Leave, GIDs: gids}
	ck.Command(args)
}

// Move向分片控制器发送Move命令
func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{Op: Move, Shard: shard, GID: gid}
	ck.Command(args)
}

// Query向分片控制器发送Query命令并返回配置
func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{Op: Query, Num: num}
	return ck.Command(args)
}

// Command向分片控制器发送命令
// 1. 尝试向当前领导者发送命令
// 2. 如果领导者错误或超时，尝试下一个服务器
// 3. 成功发送后，递增命令ID并返回配置
func (ck *Clerk) Command(args *CommandArgs) Config {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		var reply CommandReply
		// 如果调用失败（领导者错误或超时），尝试下一个服务器作为领导者
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		} else {
			ck.commandId++
			return reply.Config
		}
	}
}
