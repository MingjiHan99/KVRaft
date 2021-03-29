package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type KvOp int32
const (
	KvOp_Get KvOp = 0
	KvOp_Put KvOp = 1
	KvOp_Append KvOp = 2
)
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType KvOp 
	Key string
	Value string
	Id int64
	SeqNum int64
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	
	maxraftstate int // snapshot if log grows this big

	lastApplied int
	db map[string]string
	// Key: index Value: op
	channels map[int]chan Op
	// clients sequence number
	clients map[int64]int64 
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	kv.mu.Lock()
	seq, ok := kv.clients[args.Id]
	_, isLeader := kv.rf.GetState()
	if isLeader && ok && seq >= args.SeqNum {
		DPrintf("Server %v replies client Get(%v) seq : %v arg seq: %v",
				kv.me, args.Key, seq, args.SeqNum)
		value, exists := kv.db[args.Key]
		if exists {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return 
	} 

	kv.mu.Unlock()
	// Your code here.
	op := Op {
		OpType: KvOp_Get,
		Key: args.Key,
		Value: "",
		Id: args.Id,
		SeqNum: args.SeqNum	}
	
	index, _, isLeader := kv.rf.Start(op)

	// otherwise, push the log
	if isLeader {
		DPrintf("Server %v send Start Request in Get, Index: %v", 
			kv.me, index)
	
		kv.mu.Lock()
		resChan := make(chan Op, 1)
		kv.channels[index] = resChan
		kv.mu.Unlock()
		select {
			case op := <- resChan: {
				reply.Err = op.Err
				reply.Value = op.Value
				
				DPrintf("Server %v replies client Get(%v): %v ", kv.me, args.Key, reply.Err)
			}
			case <- time.After(time.Millisecond * 800): { 
				reply.Err = ErrWrongLeader
				DPrintf("Server %v timeout replies client Get(%v): %v ", kv.me, args.Key, reply.Err)
			}
		}

		kv.mu.Lock()
		delete(kv.channels, index)
		kv.mu.Unlock()

	} else {
		DPrintf("Server %v is not the leader now", kv.me)
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("Server %v get PutAppend Request", kv.me)
	kv.mu.Lock()
	DPrintf("Server %v get lock in PutAppend Request", kv.me)
	_, isLeader := kv.rf.GetState()

	seq, ok := kv.clients[args.Id]
	
	if isLeader && ok && seq >= args.SeqNum {
		reply.Err = OK
		DPrintf("Server replies client PutAppend(%v, %v): %v seq: %v arg seq: %v",
			 args.Key, args.Value, reply.Err, args.SeqNum, seq)
			
		kv.mu.Unlock()
		return 
	} 
	kv.mu.Unlock()

	opType := KvOp_Put
	
	if args.Op == "Append" {
		opType = KvOp_Append
	}

	op := Op {
		OpType: opType,
		Key: args.Key,
		Value: args.Value,
		Id: args.Id,
		SeqNum: args.SeqNum	}
	
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("Server %v send Start Request in PutAppend at index %v", kv.me, index)
	if isLeader {
		kv.mu.Lock()
		resChan := make(chan Op, 1)
		kv.channels[index] = resChan
		kv.mu.Unlock()
		DPrintf("Server %v waits for result in PutAppend at index %v", kv.me, index)
		select {
			case <- resChan: {
				DPrintf("Server %v replies client PutAppend(%v, %v): %v",
			 			kv.me, args.Key, args.Value, reply.Err)
					reply.Err = op.Err
			}
			case <- time.After(time.Millisecond * 800): { 
				reply.Err = ErrWrongLeader
				DPrintf("Server %v (timeout) replies client PutAppend(%v, %v): %v",
				kv.me, args.Key, args.Value, reply.Err)
			}
			
		}

		kv.mu.Lock()
		delete(kv.channels, index)
		kv.mu.Unlock()
		
	} else {
		reply.Err = ErrWrongLeader
		DPrintf("Server %v is not leader now", kv.me)
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	DPrintf("server launched\n")
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.dead = 0
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.clients = make(map[int64]int64)
	kv.channels = make(map[int]chan Op)
	kv.lastApplied = 0
	kv.readSnapshotForInit()
	
	// You may need initialization code here.
	if kv.maxraftstate != -1 {
		go kv.doSnapshot()
	}
	go kv.processLog()
	return kv
}

func (kv *KVServer) readSnapshotForInit() {
	snapshot, lastIncludedIndex := kv.rf.GetSnapshot()
	if snapshot != nil && len(snapshot) >= 1 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)

		if d.Decode(&kv.db) != nil ||
			d.Decode(&kv.clients) != nil {
			log.Fatalf("Unable to read persisted snapshot")
		}

		kv.lastApplied = lastIncludedIndex
	}
}

func (kv *KVServer) doSnapshot() {
	for !kv.killed() {
		if kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.db)
			e.Encode(kv.clients)
			data := w.Bytes()
			lastApplied := kv.lastApplied
			kv.mu.Unlock()
			DPrintf("Server %v generates snapshot at log intex %v", kv.me, lastApplied)
			kv.rf.GenerateSnapshot(data, lastApplied)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (kv *KVServer) processLog() {
	for !kv.killed() {
		msg := <- kv.applyCh
		kv.mu.Lock()
			if msg.CommandValid {
				op := msg.Command.(Op)
				
				seq, seqExists := kv.clients[op.Id]
				
				if !seqExists || seq < op.SeqNum {
					DPrintf("Server %v applied log at index %v.", kv.me, msg.CommandIndex)
					switch op.OpType {
						case KvOp_Put: {
							kv.db[op.Key] = op.Value
							op.Err = OK
							DPrintf("Op seq value:%v PUT(%v, %v)", op.SeqNum, op.Key, op.Value)
						}
						case KvOp_Get: {
							val, exists := kv.db[op.Key]
							if exists {
								op.Err = OK
								op.Value = val
							} else {
								op.Err = ErrNoKey
							}
							DPrintf("Op seq value:%v Get(%v, %v) Err (%v)", op.SeqNum, op.Key, op.Value, op.Err)
					
						}
						case KvOp_Append: {
							val, exists := kv.db[op.Key]
							if exists {
					
								kv.db[op.Key] = val + op.Value
								DPrintf("Op seq value:%v Append(%v, %v -> %v) Err (%v)", op.SeqNum, op.Key, val, kv.db[op.Key] , op.Err)
					
							} else {
								kv.db[op.Key] = op.Value
								DPrintf("Op seq value:%v Append(%v, %v) Err (%v)", op.SeqNum, op.Key, kv.db[op.Key] , op.Err)
					
							}
							op.Err = OK
						}
					}
					kv.clients[op.Id] = op.SeqNum
				
					ch, ok := kv.channels[msg.CommandIndex]
					
					delete(kv.channels, msg.CommandIndex)

					if kv.lastApplied < msg.CommandIndex {
						kv.lastApplied = msg.CommandIndex
					}

					kv.mu.Unlock()
				
					_, isLeader := kv.rf.GetState()
					if ok && isLeader {
						ch <- op
					}
					
				} else {
					kv.mu.Unlock()
				}

			} else if msg.LastIncludedIndex > kv.lastApplied {
				DPrintf("Server %v applied snapshot from %v to %v", kv.me, kv.lastApplied, msg.LastIncludedIndex)
				r := bytes.NewBuffer(msg.Command.([]byte))
				d := labgob.NewDecoder(r)

				if d.Decode(&kv.db) != nil ||
					d.Decode(&kv.clients) != nil {
					log.Fatalf("Unable to read persisted snapshot")
				}
				kv.lastApplied = msg.LastIncludedIndex
				kv.mu.Unlock()
			} else {
				kv.mu.Unlock()
			}
				
		}
}
