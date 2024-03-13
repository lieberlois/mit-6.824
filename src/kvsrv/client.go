package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{}
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := &GetArgs{
		Key: key,
	}
	reply := &GetReply{}

	for !ck.server.Call("KVServer.Get", args, reply) {
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	id := nrand()

	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		RequestId: id,
		Mode:      Write,
	}

	reply := &PutAppendReply{}

	rpcName := fmt.Sprintf("KVServer.%s", op)

	// Retry until success
	for !ck.server.Call(rpcName, args, reply) {
	}

	result := reply.Value

	// Report Success
	args.Mode = ReportSuccess
	for !ck.server.Call(rpcName, args, reply) {
	}

	// Linearizable: Wx0 -> Rx0 -> Wx1 -> Rx1
	//
	//  |---Wx0---|
	//				|-----Wx1----------(retry)--|
	// 					|---Rx0---|  							(Concurrent - Server can choose order)
	// 												|--Rx1--|	(works because Wx1 blocks/retries until success)

	return result
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
