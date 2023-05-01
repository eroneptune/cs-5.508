package kvraft

import (
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

type ApplyResult struct {
	err   Err
	value string
}

type OpId struct {
	index int
	term  int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key         string
	Value       string
	Op          string // "Get", "Put" or "Append"
	ClientId    int64
	SequenceNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied   int
	clientSession map[int64]int // state machine maintains a session for each client
	opChannel     map[OpId]chan ApplyResult
	KVDataBase    map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = KVKilled
		return
	}

	op := Op{
		Key:         args.Key,
		Value:       "",
		Op:          "Get",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		// not leader, return immediately
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.PutOpChan(index, term)
	defer kv.DeleteOpChan(index, term)

	select {
	case result := <-ch:
		reply.Err = OK
		reply.Value = result.value
		return

	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = KVKilled
		return
	}

	op := Op{
		Key:         args.Key,
		Value:       args.Value,
		Op:          args.Op,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		// not leader, return immediately
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.PutOpChan(index, term)
	defer kv.DeleteOpChan(index, term)

	select {
	case <-ch:
		reply.Err = "OK"
		return
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutOpChan(index int, term int) chan ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exists := kv.opChannel[OpId{index: index, term: term}]
	if !exists {
		kv.opChannel[OpId{index: index, term: term}] = make(chan ApplyResult, 1)
	}

	return kv.opChannel[OpId{index: index, term: term}]
}

func (kv *KVServer) DeleteOpChan(index int, term int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.opChannel, OpId{index: index, term: term})
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

func (kv *KVServer) applyMsg() {
	for kv.killed() == false {
		for msg := range kv.applyCh {
			if false {
				// snapshot
			} else {
				// append entry
				if kv.lastApplied >= msg.CommandIndex {
					// the log has already been applied
					continue
				}
				op := msg.Command.(Op)
				result := kv.applyOperation(op)
				kv.lastApplied = msg.CommandIndex
				term, isleader := kv.rf.GetState()

				if !isleader {
					continue
				}
				// notify
				ch := kv.PutOpChan(msg.CommandIndex, term)
				ch <- result
			}
		}
	}
}

func (kv *KVServer) applyOperation(op Op) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result := ApplyResult{}
	maxSeq, exists := kv.clientSession[op.ClientId]

	if !exists || maxSeq < op.SequenceNum {
		switch op.Op {
		case "Get":
			result.err = OK
			result.value = kv.KVDataBase[op.Key]
		case "Put":
			kv.KVDataBase[op.Key] = op.Value
		case "Append":
			kv.KVDataBase[op.Key] = kv.KVDataBase[op.Key] + op.Value
		default:
		}

		kv.clientSession[op.ClientId] = op.SequenceNum
		return result
	}

	if op.Op == "Get" {
		result.err = OK
		result.value = kv.KVDataBase[op.Key]
	}

	return result
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.clientSession = make(map[int64]int)
	kv.opChannel = make(map[OpId]chan ApplyResult)
	kv.KVDataBase = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyMsg()

	return kv
}
