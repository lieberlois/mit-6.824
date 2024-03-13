package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	data       map[string]string
	requestIDs map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Mode == ReportSuccess {
		delete(kv.requestIDs, args.RequestId)
		return
	}

	v, ok := kv.requestIDs[args.RequestId]
	if ok {
		DPrintf("Using result of cached request %d", args.RequestId)
		reply.Value = v
		return
	}

	newVal := args.Value

	kv.data[args.Key] = newVal
	reply.Value = newVal

	kv.requestIDs[args.RequestId] = newVal
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Mode == ReportSuccess {
		delete(kv.requestIDs, args.RequestId)
		return
	}

	v, ok := kv.requestIDs[args.RequestId]
	if ok {
		reply.Value = v
		return
	}

	old := kv.data[args.Key]
	new := fmt.Sprintf("%s%s", old, args.Value)

	kv.data[args.Key] = new

	reply.Value = old
	kv.requestIDs[args.RequestId] = old
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data:       make(map[string]string),
		requestIDs: make(map[int64]string),
	}

	return kv
}
