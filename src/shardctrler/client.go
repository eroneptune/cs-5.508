package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	clientId    int64
	leaderId    int
	sequenceNum int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.clientId = nrand()
	ck.leaderId = 0
	ck.sequenceNum = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num, SequenceNum: ck.sequenceNum, ClientId: ck.clientId}
	ck.sequenceNum++
	for {
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)

		if ok && !reply.WrongLeader {
			return reply.Config
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, SequenceNum: ck.sequenceNum, ClientId: ck.clientId}
	ck.sequenceNum++
	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)

		if ok && !reply.WrongLeader {
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, SequenceNum: ck.sequenceNum, ClientId: ck.clientId}
	ck.sequenceNum++
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)

		if ok && !reply.WrongLeader {
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, SequenceNum: ck.sequenceNum, ClientId: ck.clientId}
	ck.sequenceNum++
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)

		if ok && !reply.WrongLeader {
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
