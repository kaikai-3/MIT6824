package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"log"
	"time"
)

// 分片键值服务器
// 多个副本组，每个组运行Raft
// Shardctrler决定哪个组服务每个分片
// Shardctrler可能会不时更改分片分配

// Debug标志，用于控制调试输出
const Debug = false

// DPrintf用于调试打印
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 超时配置
const (
	ExecuteTimeout              = 500 * time.Millisecond // 命令执行超时时间
	ConfigurationMonitorTimeout = 100 * time.Millisecond // 配置监控超时时间
	MigrationMonitorTimeout     = 50 * time.Millisecond  // 迁移监控超时时间
	GCMonitorTimeout            = 50 * time.Millisecond  // 垃圾收集监控超时时间
	EmptyEntryDetectorTimeout   = 200 * time.Millisecond // 空条目检测超时时间
)

type Err uint8

const (
	OK Err = iota // 操作成功
	ErrNoKey      // 键不存在
	ErrWrongGroup // 错误的组
	ErrWrongLeader // 错误的领导者
	ErrOutDated   // 过期的配置
	ErrTimeout    // 超时
	ErrNotReady   // 尚未准备好
)

// String方法返回错误的字符串表示
func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrOutDated:
		return "ErrOutDated"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrNotReady:
		return "ErrNotReady"
	default:
		panic(fmt.Sprintf("Unknown error: %d", err))
	}
}

type ShardStatus uint8

// ShardStatus type representing the state of a shard
const (
	Serving   ShardStatus = iota // 正在服务请求
	Pulling                      // 正在从其他组拉取数据
	BePulling                    // 正在被其他组拉取
	GCing                         // 正在进行垃圾收集
)

// String方法返回分片状态的字符串表示
func (status ShardStatus) String() string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case GCing:
		return "GCing"
	default:
		panic(fmt.Sprintf("Unknown ShardStatus: %d", status))
	}
}

// CommandType representing different types of commands
type CommandType uint8

const (
	Operation     CommandType = iota // 普通操作命令
	Configuration                    // 配置变更命令
	InsertShards                     // 插入分片命令
	DeleteShards                     // 删除分片命令
	EmptyShards                      // 清空分片命令
)

// String方法返回命令类型的字符串表示
func (commandType CommandType) String() string {
	switch commandType {
	case Operation:
		return "Operation"
	case Configuration:
		return "Configuration"
	case InsertShards:
		return "InsertShards"
	case DeleteShards:
		return "DeleteShards"
	case EmptyShards:
		return "EmptyShards"
	default:
		panic(fmt.Sprintf("Unknown CommandType: %d", commandType))
	}
}

// OperationType representing various operation types
type OperationType uint8

const (
	Get OperationType = iota // 获取操作
	Put                    // 设置操作
	Append                 // 追加操作
)

// String方法返回操作类型的字符串表示
func (op OperationType) String() string {
	switch op {
	case Get:
		return "Get"
	case Put:
		return "Put"
	case Append:
		return "Append"
	default:
		panic(fmt.Sprintf("Unknown OperationType: %d", op))
	}
}

// CommandArgs表示命令参数结构体
// 包含键、值、操作类型、客户端ID和命令ID
// clientId和commandId组合唯一标识一个操作

type CommandArgs struct {
	Key       string      // 键
	Value     string      // 值
	Op        OperationType // 操作类型
	ClientId  int64       // 客户端ID
	CommandId int64       // 命令ID
}

// String方法返回命令参数的字符串表示
func (args *CommandArgs) String() string {
	return fmt.Sprintf("CommandArgs{Key: %s, Value: %s, Op: %s, ClientId: %d, CommandId: %d}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

// CommandReply表示命令执行的回复结构体
// 包含错误码和返回值
type CommandReply struct {
	Err   Err    // 错误码
	Value string // 返回值
}

// String方法返回命令回复的字符串表示
func (reply *CommandReply) String() string {
	return fmt.Sprintf("CommandReply{Err: %s, Value: %s}", reply.Err, reply.Value)
}

// OperationContext表示操作的执行上下文
// 用于检测重复请求
type OperationContext struct {
	MaxAppliedCommandId int64      // 最大已应用的命令ID
	LastReply           *CommandReply // 最后一次回复
}

// String方法返回操作上下文的字符串表示
func (operationContext OperationContext) String() string {
	return fmt.Sprintf("OperationContext{MaxAppliedCommandId: %d, LastReply: %v}", operationContext.MaxAppliedCommandId, operationContext.LastReply)
}

// deepCopy创建OperationContext的深拷贝
func (operationContext OperationContext) deepCopy() OperationContext {
	return OperationContext{
		MaxAppliedCommandId: operationContext.MaxAppliedCommandId,
		LastReply: &CommandReply{
			Err:   operationContext.LastReply.Err,
			Value: operationContext.LastReply.Value},
	}
}

// ShardOperationArgs表示分片操作的参数结构体
// 用于分片迁移和垃圾收集操作
type ShardOperationArgs struct {
	ConfigNum int   // 配置编号
	ShardIDs  []int // 分片ID列表
}

// String方法返回分片操作参数的字符串表示
func (args *ShardOperationArgs) String() string {
	return fmt.Sprintf("ShardOperationArgs{ConfigNum: %d, ShardIDs: %v}", args.ConfigNum, args.ShardIDs)
}

// ShardOperationReply表示分片操作的回复结构体
// 用于分片迁移和垃圾收集操作的回复
type ShardOperationReply struct {
	Err            Err // 错误码
	ConfigNum      int // 配置编号
	Shards         map[int]map[string]string // 分片数据
	LastOperations map[int64]OperationContext // 最后操作上下文
}

// String方法返回分片操作回复的字符串表示
func (reply *ShardOperationReply) String() string {
	return fmt.Sprintf("ShardOperationReply{Err: %s, ConfigNum: %d, Shards: %v, LastOperations: %v}", reply.Err, reply.ConfigNum, reply.Shards, reply.LastOperations)
}

// Command表示要执行的命令结构体
// 包含命令类型和数据
type Command struct {
	CommandType CommandType // 命令类型
	Data        interface{} // 命令数据
}

// String方法返回命令的字符串表示
func (command Command) String() string {
	return fmt.Sprintf("Command{commandType: %s, Data: %v}", command.CommandType, command.Data)
}

// NewOperationCommand创建新的操作命令
func NewOperationCommand(args *CommandArgs) Command {
	return Command{Operation, *args}
}

// NewConfigurationCommand创建新的配置命令
func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

// NewInsertShardsCommand创建新的插入分片命令
func NewInsertShardsCommand(reply *ShardOperationReply) Command {
	return Command{InsertShards, *reply}
}

// NewDeleteShardsCommand创建新的删除分片命令
func NewDeleteShardsCommand(args *ShardOperationArgs) Command {
	return Command{DeleteShards, *args}
}

// NewEmptyShardsCommand创建新的空分片命令
func NewEmptyShardsCommand() Command {
	return Command{EmptyShards, nil}
}
