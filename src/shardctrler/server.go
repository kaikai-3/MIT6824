package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

// ShardCtrler是shard系统的控制器，负责管理分片的分配和迁移
type ShardCtrler struct {
	mu      sync.RWMutex       // 读写锁，用于保护共享数据
	rf      *raft.Raft         // Raft实例，用于确保日志的一致性
	applyCh chan raft.ApplyMsg // 通道，用于接收Raft实例应用的日志消息

	// 其他数据成员
dead           int32                      // 由Kill()设置，表示ShardCtrler实例是否已终止
	stateMachine   ConfigStateMachine         // 状态机，用于存储shard系统的配置
	lastOperations map[int64]OperationContext // 记录上次操作上下文，用于避免重复请求
	notifyChans    map[int]chan *CommandReply // 通知通道，用于通知客户端goroutine
}

// Command处理来自客户端的命令
// 如果命令不是查询且是重复请求，返回之前的回复
// 否则，尝试在Raft层启动命令
// 如果当前服务器不是leader，返回错误
// 如果命令成功启动，等待结果或超时
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.RLock()
	// 检查是否为重复请求（仅针对非查询命令）
	if args.Op != Query && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		LastReply := sc.lastOperations[args.ClientId].LastReply
		reply.Config, reply.Err = LastReply.Config, LastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	// 在Raft层启动命令
	index, _, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	// 获取通知通道以等待结果
	notifyChan := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-notifyChan:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	// 异步清理过期的通知通道
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()
}

// isDuplicateRequest检查客户端的命令是否为重复请求
func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	OperationContext, ok := sc.lastOperations[clientId]
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

// getNotifyChan获取指定索引的通知通道
// 如果通道不存在，则创建一个新的
func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	notifyChan, ok := sc.notifyChans[index]
	if !ok {
		notifyChan = make(chan *CommandReply, 1)
		sc.notifyChans[index] = notifyChan
	}
	return notifyChan
}

// removeOutdatedNotifyChan移除指定索引的过期通知通道
func (sc *ShardCtrler) removeOutdatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

// applier是goroutine，用于将日志应用到状态机
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				reply := new(CommandReply)
				command := message.Command.(Command)
				sc.mu.Lock()

				// 处理重复请求
				if command.Op != Query && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = sc.lastOperations[command.ClientId].LastReply
				} else {
					reply = sc.applyLogToStateMachine(command)
					if command.Op != Query {
						sc.lastOperations[command.ClientId] = OperationContext{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}

				// 当节点是leader时，只通知当前任期的日志相关通道
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					notifyChan := sc.getNotifyChan(message.CommandIndex)
					notifyChan <- reply
				}
				sc.mu.Unlock()
			}
		}
	}
}

// applyLogToStateMachine将命令应用到状态机并返回回复
func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case Join:
		reply.Err = sc.stateMachine.Join(command.Servers)
	case Leave:
		reply.Err = sc.stateMachine.Leave(command.GIDs)
	case Move:
		reply.Err = sc.stateMachine.Move(command.Shard, command.GID)
	case Query:
		reply.Config, reply.Err = sc.stateMachine.Query(command.Num)
	}
	return reply
}

// Kill()由测试器调用，当ShardCtrler实例不再需要时
// 你不需要在Kill()中做任何事情，但为了方便，可以
// 关闭这个实例的调试输出
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// 如果需要，可以在这里添加代码
	atomic.StoreInt32(&sc.dead, 1)
}

// killed检查ShardCtrler实例是否已被终止
func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// Raft返回底层的Raft实例，由shardkv测试器需要
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer启动shardctrler服务
// servers[]包含一组服务器的端口，这些服务器将通过Raft协作
// 形成容错的shardctrler服务
// me是当前服务器在servers[]中的索引
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	apply := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		rf:             raft.Make(servers, me, persister, apply),
		applyCh:        apply,
		stateMachine:   NewMemoryConfigStateMachine(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
		dead:           0,
	}
	go sc.applier()
	return sc
}
