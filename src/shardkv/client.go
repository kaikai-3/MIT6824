package shardkv

// 客户端代码，用于与分片键值服务通信
// 客户端首先与shardctrler通信以获取分片到组的映射，然后与持有键分片的组通信

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"

// key2shard根据键计算分片ID
// 请使用此函数，不要修改它
// 通过取键的第一个字符的ASCII值，然后对NShards取模来计算分片ID
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// nrand生成一个随机的64位整数
// 用于生成唯一的客户端ID
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Clerk是客户端结构体，用于与shardkv服务通信
type Clerk struct {
	sm      *shardctrler.Clerk             // 与shardctrler通信的客户端，用于获取最新的配置数据（分片到组的映射）
	config  shardctrler.Config             // 当前集群配置，包括分片到组的映射，客户端根据此配置发送请求
	makeEnd func(string) *labrpc.ClientEnd // 生成RPC连接到服务器的函数，每个服务器通过唯一地址标识
	leaderIds map[int]int // gid -> leaderId，gid是组ID，leaderId是组中的leader服务器ID
	clientId  int64       // 通过nrand()生成的客户端ID，最好使用保证无冲突的分布式ID生成算法
	commandId int64       // (clientId, commandId)唯一定义一个操作
}

// MakeClerk创建一个新的客户端实例
// 测试器调用此函数
// ctrlers[]用于创建shardctrler的客户端
// makeEnd用于将服务器名称转换为可以发送RPC的ClientEnd
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		makeEnd:   makeEnd,
		leaderIds: make(map[int]int),
		clientId:  nrand(),
		commandId: 0,
	}
	// 从shardctrler查询最新的配置
	ck.config = ck.sm.Query(-1)
	return ck
}

// Get是Command函数的包装器，用于获取键的值
func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: Get})
}

// Put是Command函数的包装器，用于设置键的值
func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Put})
}

// Append是Command函数的包装器，用于追加值到键
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Append})
}

// Command处理客户端的命令请求
func (ck *Clerk) Command(args *CommandArgs) string {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		// 计算键所属的分片ID
		shard := key2shard(args.Key)
		// 获取该分片所属的组ID
		gid := ck.config.Shards[shard]
		// 获取该组的服务器列表
		if servers, ok := ck.config.Groups[gid]; ok {
			// 如果没有设置leaderId，默认设置为0
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			// 获取当前的leaderId
			oldLeaderId := ck.leaderIds[gid]
			// 从当前leader开始尝试
			newLeader := oldLeaderId
			for {
				reply := new(CommandReply)
				// 向leader服务器发送请求
				ok := ck.makeEnd(servers[newLeader]).Call("ShardKV.Command", args, reply)
				// 如果请求成功且返回OK或ErrNoKey，返回结果
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.commandId++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					// 如果返回ErrWrongGroup，退出当前组的循环
					break
				} else {
					// 尝试下一个服务器
					newLeader = (newLeader + 1) % len(servers)
					// 如果所有服务器都已尝试过，退出循环
					if newLeader == oldLeaderId {
						break
					}
				}
			}
		}
		// 等待一段时间后，查询最新的配置
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
