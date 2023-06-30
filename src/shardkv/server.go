package shardkv

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type ApplyResult struct {
	Err   Err
	Value string
}

type OpId struct {
	index int
	term  int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Op          string // "PutAppend" or "Get"
	ClientId    int64
	SequenceNum int
	Key         string
	Value       string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	mck       *shardctrler.Clerk
	opChannel map[OpId]chan ApplyResult

	lastApplied int

	// persist
	kvDataBase      map[string]string
	clientSession   map[int64]int // state machine maintains a session for each client
	configs         []shardctrler.Config
	previousData    map[int]map[int]map[string]string // configNum -> shard -> data
	unmigratedShard map[int]int                       // shardId -> configNum
	servings        map[int]bool                      // sering shards shardId -> bool
	garbages        map[int]map[int]bool

	killed bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Op: "Get", ClientId: args.ClientId, SequenceNum: args.SequenceNum, Key: args.Key, Value: ""}
	reply.Err, reply.Value = kv.operationHandler(op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{Op: args.Op, ClientId: args.ClientId, SequenceNum: args.SequenceNum, Key: args.Key, Value: args.Value}
	reply.Err, _ = kv.operationHandler(op)
}

func (kv *ShardKV) Migration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.ShardId, reply.ConfigNum = ErrWrongLeader, args.ShardId, args.ConfigNum
	if _, leader := kv.rf.GetState(); !leader {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= len(kv.configs)-1 {
		return
	}

	reply.Err = OK
	reply.Data, reply.ClientSession = kv.codyData(args.ConfigNum, args.ShardId)

}

func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	if _, leader := kv.rf.GetState(); !leader {
		return
	}

	kv.mu.Lock()

	if _, exists := kv.previousData[args.ConfigNum]; !exists {
		kv.mu.Unlock()
		return
	}

	if _, exists := kv.previousData[args.ConfigNum][args.ShardId]; !exists {
		kv.mu.Unlock()
		return
	}

	gcOp := Op{"GC", 0, 0, strconv.Itoa(args.ConfigNum), strconv.Itoa(args.ShardId)}
	kv.mu.Unlock()

	reply.Err, _ = kv.operationHandler(gcOp)
}

func (kv *ShardKV) codyData(configNum int, shardId int) (map[string]string, map[int64]int) {
	dbCopy := make(map[string]string)
	for key, value := range kv.previousData[configNum][shardId] {
		dbCopy[key] = value
	}

	csCopy := make(map[int64]int)
	for clientId, seqNum := range kv.clientSession {
		csCopy[clientId] = seqNum
	}

	return dbCopy, csCopy
}

func (kv *ShardKV) operationHandler(op Op) (Err, string) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		// wrong leader
		return ErrWrongLeader, ""
	}

	ch := kv.GetOpChan(index, term, true)
	defer kv.DeleteOpChan(index, term)

	select {
	case result := <-ch:
		return result.Err, result.Value
	case <-time.After(time.Second):
		// timeout
		return ErrWrongLeader, ""
	}
}

func (kv *ShardKV) GetOpChan(index int, term int, create bool) chan ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exists := kv.opChannel[OpId{index: index, term: term}]
	if !exists && create {
		kv.opChannel[OpId{index: index, term: term}] = make(chan ApplyResult, 1)
	}

	return kv.opChannel[OpId{index: index, term: term}]
}

func (kv *ShardKV) DeleteOpChan(index int, term int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.opChannel, OpId{index: index, term: term})
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvDataBase map[string]string
	var clientSession map[int64]int
	var configs []shardctrler.Config
	var previousData map[int]map[int]map[string]string
	var unmigratedShard map[int]int
	var servings map[int]bool
	var garbages map[int]map[int]bool

	if d.Decode(&kvDataBase) != nil ||
		d.Decode(&clientSession) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&previousData) != nil ||
		d.Decode(&unmigratedShard) != nil ||
		d.Decode(&servings) != nil ||
		d.Decode(&garbages) != nil {
		// error...
	} else {
		kv.kvDataBase = kvDataBase
		kv.clientSession = clientSession
		kv.configs = configs
		kv.previousData = previousData
		kv.unmigratedShard = unmigratedShard
		kv.servings = servings
		kv.garbages = garbages
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	kv.killed = true
	kv.mu.Unlock()
}

func (kv *ShardKV) updateSnapshot(index int) {
	kv.mu.Lock()
	if kv.maxraftstate == -1 {
		kv.mu.Unlock()
		return
	}

	threshold := 10
	if kv.maxraftstate-kv.persister.RaftStateSize() > kv.maxraftstate/threshold {
		kv.mu.Unlock()
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDataBase)
	e.Encode(kv.clientSession)
	e.Encode(kv.configs)
	e.Encode(kv.previousData)
	e.Encode(kv.unmigratedShard)
	e.Encode(kv.servings)
	e.Encode(kv.garbages)
	snapshot := w.Bytes()
	kv.mu.Unlock()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) applyMsg() {
	for {
		if kv.killed {
			break
		}

		for msg := range kv.applyCh {
			if msg.SnapshotValid {
				// snapshot
				if kv.lastApplied >= msg.SnapshotIndex {
					// the log has already been applied
					continue
				}
				// install snapshot
				kv.installSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
				continue
			}

			if kv.lastApplied >= msg.CommandIndex {
				// the log has already been applied
				continue
			}

			if config, ok := msg.Command.(shardctrler.Config); ok {
				// synchronize next config
				kv.updateConfig(config)

			} else if migrateReply, ok := msg.Command.(MigrateReply); ok {
				// synchronize migrate data
				kv.saveMigrateData(migrateReply)

			} else {
				op := msg.Command.(Op)
				result := kv.applyOperation(op)

				term, isleader := kv.rf.GetState()
				if isleader {
					if ch := kv.GetOpChan(msg.CommandIndex, term, false); ch != nil {
						ch <- result
					}
				}
			}
			kv.lastApplied = msg.CommandIndex
			go kv.updateSnapshot(msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) updateConfig(newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(kv.configs) != newConfig.Num {
		// must the next config
		return
	}

	lastConfig, dataToMigrateOut := kv.configs[len(kv.configs)-1], kv.servings
	kv.servings = make(map[int]bool)
	kv.configs = append(kv.configs, newConfig)
	for sid, gid := range newConfig.Shards {
		if gid != kv.gid {
			// not belong to me
			continue
		}

		if _, exists := dataToMigrateOut[sid]; exists || lastConfig.Num == 0 {
			// has owned the shard, no need to migrate, keep serving
			kv.servings[sid] = true
			delete(dataToMigrateOut, sid)
		} else {
			// need migrate in
			kv.unmigratedShard[sid] = lastConfig.Num
		}
	}

	if len(dataToMigrateOut) > 0 {
		// there exists some data to migrate out in last config
		kv.previousData[lastConfig.Num] = make(map[int]map[string]string)
		for shard, _ := range dataToMigrateOut {
			copyDb := make(map[string]string)
			for key, value := range kv.kvDataBase {
				if key2shard(key) == shard {
					copyDb[key] = value
					delete(kv.kvDataBase, key)
				}
			}
			kv.previousData[lastConfig.Num][shard] = copyDb
		}
	}
	go kv.fetchData()
}

func (kv *ShardKV) saveMigrateData(reply MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.ConfigNum+1 != len(kv.configs)-1 {
		// the data must be latest
		return
	}

	if _, exists := kv.servings[reply.ShardId]; !exists {
		// clear all the leftover data
		for key, _ := range kv.kvDataBase {
			if key2shard(key) == reply.ShardId {
				delete(kv.kvDataBase, key)
			}
		}
		// install data
		for key, value := range reply.Data {
			kv.kvDataBase[key] = value
		}
		// update client request sequence
		for clientId, seqNum := range reply.ClientSession {
			kv.clientSession[clientId] = Max(kv.clientSession[clientId], seqNum)
		}
		// commit
		kv.servings[reply.ShardId] = true
		// garbage
		if _, exists := kv.garbages[reply.ConfigNum]; !exists {
			kv.garbages[reply.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[reply.ConfigNum][reply.ShardId] = true
	}
	// shard start serving
	delete(kv.unmigratedShard, reply.ShardId)
}

func (kv *ShardKV) applyOperation(op Op) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result := ApplyResult{}

	if op.Op == "GC" {
		cfgNum, _ := strconv.Atoi(op.Key)
		shard, _ := strconv.Atoi(op.Value)

		if _, exists := kv.previousData[cfgNum]; exists {
			delete(kv.previousData[cfgNum], shard)
			if len(kv.previousData[cfgNum]) == 0 {
				delete(kv.previousData, cfgNum)
			}
		}
		result.Err = OK
		return result
	}

	shard := key2shard(op.Key)
	if _, exists := kv.servings[shard]; !exists {
		result.Err = ErrWrongGroup
		return result
	}

	result.Err = OK
	maxSeq, exists := kv.clientSession[op.ClientId]
	if !exists || maxSeq < op.SequenceNum {
		if op.Op == "Put" {
			kv.kvDataBase[op.Key] = op.Value
		} else if op.Op == "Append" {
			kv.kvDataBase[op.Key] = kv.kvDataBase[op.Key] + op.Value
		}
		kv.clientSession[op.ClientId] = op.SequenceNum
	}

	if op.Op == "Get" {
		result.Value = kv.kvDataBase[op.Key]
		kv.clientSession[op.ClientId] = Max(kv.clientSession[op.ClientId], op.SequenceNum)
	}

	return result
}

func (kv *ShardKV) fetchLatestConfig() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.unmigratedShard) > 0 { // 本轮移入还未完成
		kv.mu.Unlock()
		return
	}
	// fetch the next config
	kv.mu.Unlock()
	nextConfig := kv.mck.Query(len(kv.configs))
	if nextConfig.Num == len(kv.configs) {
		kv.rf.Start(nextConfig)
	}
}

func (kv *ShardKV) fetchData() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.unmigratedShard) == 0 { // 本轮移入已经完成
		kv.mu.Unlock()
		return
	}

	var wait sync.WaitGroup
	for sid, configNum := range kv.unmigratedShard {
		wait.Add(1)
		go func(shardId int, config shardctrler.Config) {
			defer wait.Done()
			args := MigrateArgs{ShardId: shardId, ConfigNum: config.Num}
			gid := config.Shards[shardId]
			for _, server := range config.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				if ok := srv.Call("ShardKV.Migration", &args, &reply); ok {
					if reply.Err == OK {
						kv.rf.Start(reply)
					}
				}

			}
		}(sid, kv.configs[configNum])
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) garbagesCollection() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0 { // 没有垃圾
		kv.mu.Unlock()
		return
	}

	var wait sync.WaitGroup
	for configNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(shardId int, config shardctrler.Config) {
				defer wait.Done()
				args := MigrateArgs{ShardId: shardId, ConfigNum: config.Num}
				gid := config.Shards[shardId]
				for _, server := range config.Groups[gid] {
					srv := kv.make_end(server)
					reply := MigrateReply{}
					if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						delete(kv.garbages[config.Num], shardId)
						if len(kv.garbages) == 0 {
							delete(kv.garbages, config.Num)
						}
						kv.mu.Unlock()
					}

				}
			}(shard, kv.configs[configNum])
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		// kill
		kv.mu.Lock()
		if kv.killed {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()

		do()
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killed = false

	// Your initialization code here.
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.opChannel = make(map[OpId]chan ApplyResult)
	kv.clientSession = make(map[int64]int)
	kv.lastApplied = 0
	kv.kvDataBase = make(map[string]string)
	kv.configs = make([]shardctrler.Config, 1)
	kv.configs[0].Num = 0
	kv.previousData = make(map[int]map[int]map[string]string)
	kv.unmigratedShard = make(map[int]int)
	kv.servings = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)
	kv.installSnapshot(kv.persister.ReadSnapshot())

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsg()
	go kv.daemon(kv.fetchLatestConfig, 20)
	go kv.daemon(kv.fetchData, 20)
	go kv.daemon(kv.garbagesCollection, 60)

	return kv
}
