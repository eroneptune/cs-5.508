package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SequenceNum int
	ClientId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SequenceNum int
	ClientId    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	ShardId   int
	ConfigNum int
}

type MigrateReply struct {
	Err           Err
	ShardId       int
	ConfigNum     int
	Data          map[string]string
	ClientSession map[int64]int
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
