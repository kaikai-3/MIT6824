package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int            // snapshot if log grows this big
	lastApplied  int            //最后一次提交的index
	stateMachine KVStateMachine //状态机
	//上一次操作的上下文信息
	lastOperations map[int64]OperationContext
	//raft返回值的channel
	notifyChs map[int]chan *CommandReply
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isDuplicatedCommand(clientId, commandId int64) bool {
	OperationContext, ok := kv.lastOperations[clientId]
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	if args.Op != OpGet && kv.isDuplicatedCommand(args.ClientId, args.CommandId) {
		//直接返回缓存
		LastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = LastReply.Value, LastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout)://执行命令超时
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)

	switch command.Op {
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine.Get(command.Key)
	case OpAppend:
		reply.Err = kv.stateMachine.Append(command.Key, command.Value)
	case OpPut:
		reply.Err = kv.stateMachine.Put(command.Key, command.Value)
	}
	return reply
}

func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) appler() {
	//一直监听raft提交的消息通道
	for kv.killed() == false {
		select {
		//当收到一条提交消息
		case message := <-kv.applyCh:
			//判断消息是否合法，raft会向状态机提交不合法的消息吗？
			if message.CommandValid {
				kv.mu.Lock()
				//过滤掉已经提交的日志
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex
				reply := new(CommandReply)
				command := message.Command.(Command)
				//如果是写操作的重复命令
				if command.Op != OpGet && kv.isDuplicatedCommand(command.ClientId, command.CommandId) {
					reply = kv.lastOperations[command.ClientId].LastReply
				} else {
					reply = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}
				//确保还在当前leader的任期里面
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <- reply //通知返回客户端
				}
				//判读是否需要快照功能
				if kv.needSnapshot() {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm,message.SnapshotIndex,message.Snapshot){
					kv.restoreStateFromSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			}else {
				panic(fmt.Sprintf("Invalid ApplyMsg %v", message))
			}
		}
	}
}

// needSnapshot 判断是否需要快照
func (kv *KVServer) needSnapshot() bool{
	//设置了raft节点的最大日志并且日志量以及超过了最大日志量
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

// takeSnapshot 执行快照到日志index
func (kv *KVServer) takeSnapshot(index int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	data := w.Bytes()
	kv.rf.Snapshot(index,data)
}

// restoreStateFromSnapshot 从快照中恢复状态机数据
func (kv *KVServer) restoreStateFromSnapshot(snapshot []byte){
	if snapshot == nil || len(snapshot) < 1 {
		return 
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine KV_dataset
	var lastOperations map[int64]OperationContext
	if d.Decode(&stateMachine) != nil || d.Decode(&lastOperations) != nil{
		panic("Failed to restore state from snapshot")
	}
	kv.stateMachine = &stateMachine
	kv.lastOperations = lastOperations
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		mu:             sync.RWMutex{},
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		dead:           0,
		maxraftstate:   maxraftstate,
		stateMachine:   &KV_dataset{KV: make(map[string]string)},
		lastOperations: make(map[int64]OperationContext),
		notifyChs:      make(map[int]chan *CommandReply),
	}

	kv.restoreStateFromSnapshot(persister.ReadSnapshot())
	//开启服务器的提交协程
	go kv.appler()

	return kv
}
