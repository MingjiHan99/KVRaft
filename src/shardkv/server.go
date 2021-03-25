package shardkv


import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "time"
import "log"
import "bytes"
import "sync/atomic"
//import "fmt"
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
	KvOp_Config KvOp = 3
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

	Config shardmaster.Config
}


type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	
	// Your definitions here.
	dead    int32 
	lastApplied int
	db map[string]string
	// Key: index Value: op
	channels map[int]chan Op
	// clients sequence number
	clients map[int64]int64 
	configs []shardmaster.Config
	pdclient *shardmaster.Clerk
}
// use it with lock, be careful :)
func (kv *ShardKV) lastestConfig() shardmaster.Config {
	return kv.configs[len(kv.configs) - 1]
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
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
				_, isLeader := kv.rf.GetState()
				if isLeader {
					reply.Err = op.Err
					reply.Value = op.Value
				} else {
					reply.Err = ErrWrongLeader
				}
				
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
				_, isLeader := kv.rf.GetState()
				if isLeader {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
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
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.clients = make(map[int64]int64)
	kv.channels = make(map[int]chan Op)
	kv.lastApplied = 0
	kv.configs = make([]shardmaster.Config, 1)
	kv.configs[0].Groups = map[int][]string{}
	kv.configs[0].Num = 0
	kv.pdclient = shardmaster.MakeClerk(kv.masters)
	kv.readSnapshotForInit()


	if kv.maxraftstate != -1 {
		go kv.doSnapshot()
	}
	go kv.pullConfig()
	go kv.processLog()

	return kv
}

func (kv *ShardKV) readSnapshotForInit() {
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

func (kv *ShardKV) pullConfig() {
	for !kv.killed() {
		//only leader can take the configuration
	    if _, isLeader := kv.rf.GetState(); isLeader {
			nextNum := kv.lastestConfig().Num + 1
			cfg := kv.pdclient.Query(nextNum)
			if cfg.Num == nextNum {
				op := Op {
					OpType: KvOp_Config,
					Config: cfg }
				kv.rf.Start(op)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}


func (kv *ShardKV) doSnapshot() {
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

func (kv *ShardKV) processLog() {
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

						}
						case KvOp_Config: {
							if op.Config.Num == kv.lastestConfig().Num +  1 {
								kv.configs = append(kv.configs, op.Config)
							}
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
