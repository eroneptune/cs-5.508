package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
	// You'll have to add code here.

	ck.clientId = nrand()
	ck.leaderId = 0
	ck.sequenceNum = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	req := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.sequenceNum++

	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &req, &reply)

		if reply.Err == "OK" {
			return reply.Value
		}

		if !ok || reply.Err == "ErrWrongLeader" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	req := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.sequenceNum++

	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &req, &reply)

		if reply.Err == "OK" {
			return
		}

		if !ok || reply.Err == "ErrWrongLeader" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
