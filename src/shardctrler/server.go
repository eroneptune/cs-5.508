package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opChannel     map[int]chan Op
	clientSession map[int64]int // state machine maintains a session for each client
	lastApplied   int

	configs []Config // indexed by config num
	killed  bool
}

type ShardPerGroup struct {
	GroupID int
	Shards  []int
}

type Op struct {
	// Your data here.
	Op          string // "Join", "Leave", "Move" or "Query"
	ClientId    int64
	SequenceNum int
	Args        interface{}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Op: "Join", ClientId: args.ClientId, SequenceNum: args.SequenceNum, Args: *args}
	reply.WrongLeader, _ = sc.operationHandler(op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Op: "Leave", ClientId: args.ClientId, SequenceNum: args.SequenceNum, Args: *args}
	reply.WrongLeader, _ = sc.operationHandler(op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Op: "Move", ClientId: args.ClientId, SequenceNum: args.SequenceNum, Args: *args}
	reply.WrongLeader, _ = sc.operationHandler(op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Op: "Query", ClientId: args.ClientId, SequenceNum: args.SequenceNum, Args: *args}
	config := Config{}
	reply.WrongLeader, config = sc.operationHandler(op)
	if !reply.WrongLeader {
		reply.Config = config
	}
}

func (sc *ShardCtrler) operationHandler(op Op) (bool, Config) {
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		// wrong leader
		return true, Config{}
	}

	ch := sc.GetOpChan(index, true)
	defer sc.DeleteOpChan(index)

	select {
	case newOp := <-ch:
		if !sc.opEqual(op, newOp) {
			return true, Config{}
		}

		if op.Op != "Query" {
			return false, Config{}
		}

		return false, newOp.Args.(Config)
	case <-time.After(time.Second):
		// timeout
		return true, Config{}
	}
}

func (sc *ShardCtrler) opEqual(oldOp Op, newOp Op) bool {
	return oldOp.SequenceNum == newOp.SequenceNum && oldOp.ClientId == newOp.ClientId && oldOp.Op == newOp.Op
}

func (sc *ShardCtrler) GetOpChan(index int, create bool) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, exists := sc.opChannel[index]
	if !exists && create {
		sc.opChannel[index] = make(chan Op, 1)
	}

	return sc.opChannel[index]
}

func (sc *ShardCtrler) DeleteOpChan(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.opChannel, index)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.mu.Lock()
	sc.killed = true
	sc.mu.Unlock()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyOperation(op *Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	maxSeq, exists := sc.clientSession[op.ClientId]
	if !exists || maxSeq < op.SequenceNum {
		switch op.Op {
		case "Join":
			newConfig := sc.newConfig()
			joinArg := op.Args.(JoinArgs)
			for gid, servers := range joinArg.Servers {
				newServers := make([]string, len(servers))
				copy(newServers, servers)
				newConfig.Groups[gid] = newServers
			}
			sc.balance(&newConfig)
			sc.configs = append(sc.configs, newConfig)
			break

		case "Leave":
			newConfig := sc.newConfig()
			leaveArg := op.Args.(LeaveArgs)
			for _, gid := range leaveArg.GIDs {
				delete(newConfig.Groups, gid)
				// free shards
				for i := 0; i < NShards; i++ {
					if newConfig.Shards[i] == gid {
						newConfig.Shards[i] = 0
					}
				}
			}
			sc.balance(&newConfig)
			sc.configs = append(sc.configs, newConfig)
			break

		case "Move":
			newConfig := sc.newConfig()
			moveArg := op.Args.(MoveArgs)
			if moveArg.Shard < 0 || moveArg.Shard > NShards {
				// error
				break
			}

			_, exists := newConfig.Groups[moveArg.GID]
			if !exists {
				// not exist
				break
			}

			newConfig.Shards[moveArg.Shard] = moveArg.GID
			sc.configs = append(sc.configs, newConfig)
			break

		case "Query":
			// do nothing

		}
		sc.clientSession[op.ClientId] = op.SequenceNum
	}

	if op.Op == "Query" {
		queryArgs := op.Args.(QueryArgs)
		if queryArgs.Num == -1 || queryArgs.Num >= len(sc.configs) {
			// the latest configuration
			op.Args = sc.configs[len(sc.configs)-1]
		} else {
			op.Args = sc.configs[queryArgs.Num]
		}
	}

	return false
}

func (sc *ShardCtrler) balance(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
		return
	}

	shardsCount := sc.shardsPerGroup(config)
	for {
		maxLoad, minLoad := sc.getMaxAndMinLoad(shardsCount)

		if len(shardsCount[maxLoad].Shards)-len(shardsCount[minLoad].Shards) <= 1 {
			// already balance
			break
		}

		config.Shards[shardsCount[maxLoad].Shards[0]] = shardsCount[minLoad].GroupID
		shardsCount[minLoad].Shards = append(shardsCount[minLoad].Shards, shardsCount[maxLoad].Shards[0])
		shardsCount[maxLoad].Shards = shardsCount[maxLoad].Shards[1:]
	}
}

func (sc *ShardCtrler) shardsPerGroup(config *Config) []ShardPerGroup {
	result := make([]ShardPerGroup, 0)

	// 对 gid 进行排序
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, gid := range gids {
		exists := false
		for i := 0; i < len(result); i++ {
			if result[i].GroupID == gid {
				exists = true
				break
			}
		}

		if !exists {
			result = append(result, ShardPerGroup{
				GroupID: gid,
				Shards:  []int{},
			})
		}
	}

	// assign all the free shard to the first group
	for i := 0; i < len(config.Shards); i++ {
		if config.Shards[i] == 0 {
			config.Shards[i] = gids[0]
		}
	}

	for sid, gid := range config.Shards {
		for i := 0; i < len(result); i++ {
			if result[i].GroupID == gid {
				result[i].Shards = append(result[i].Shards, sid)
				break
			}
		}
	}
	return result
}

func (sc *ShardCtrler) getMaxAndMinLoad(shardsCount []ShardPerGroup) (int, int) {
	max := -1
	min := NShards + 1
	var maxLoad, minLoad int

	for idx, elem := range shardsCount {
		if len(elem.Shards) > max {
			max = len(elem.Shards)
			maxLoad = idx
		}

		if len(elem.Shards) < min {
			min = len(elem.Shards)
			minLoad = idx
		}
	}

	return maxLoad, minLoad
}

func (sc *ShardCtrler) newConfig() Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{lastConfig.Num + 1, lastConfig.Shards, make(map[int][]string)}
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}

func (sc *ShardCtrler) applyMsg() {
	for {
		sc.mu.Lock()
		if sc.killed {
			sc.mu.Unlock()
			break
		}
		sc.mu.Unlock()

		select {
		case msg := <-sc.applyCh:

			if msg.SnapshotValid {
				// snapshot
				continue
			}

			if sc.lastApplied >= msg.CommandIndex {
				// the log has already been applied
				continue
			}

			op := msg.Command.(Op)
			sc.applyOperation(&op)
			sc.lastApplied = msg.CommandIndex

			_, isleader := sc.rf.GetState()
			if !isleader {
				continue
			}

			if ch := sc.GetOpChan(msg.CommandIndex, false); ch != nil {
				ch <- op
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.clientSession = make(map[int64]int)
	sc.opChannel = make(map[int]chan Op)

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.applyMsg()

	return sc
}
