package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	KeyValue map[string]string
	DupOps   map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.KeyValue[args.Key]
	// if kv.DupOps[args.Uid] != "" {
	// 	return
	// }
	// kv.DupOps[args.Uid] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	old := kv.KeyValue[args.Key]
	if old != args.Value {
		kv.KeyValue[args.Key] = args.Value
	}
	// kv.DupOps[args.Uid] = "1"
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.DupOps[args.Uid]; ok {
		reply.Value = kv.DupOps[args.Uid]
		return
	}
	old := kv.KeyValue[args.Key]
	kv.KeyValue[args.Key] = old + args.Value
	kv.DupOps[args.Uid] = old
	reply.Value = kv.DupOps[args.Uid]
}

func (kv *KVServer) DeletePut(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.DupOps, args.Uid)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.KeyValue = make(map[string]string)
	kv.DupOps = make(map[string]string)
	// You may need initialization code here.

	return kv
}
