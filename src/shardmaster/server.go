package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "log"
import "time"
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killed bool
	configs []Config // indexed by config num
	// record the timestamps
	clients map[int64]int64
	// index in Raft to reply channel
	channels map[int]chan Op
}

type OpType int32

const (
	OpType_Join OpType = 1
	OpType_Leave OpType = 2
	OpType_Move OpType = 3
	OpType_Query OpType = 4
)	

type Op struct {
	
	// Your data here.
	Type OpType
	// Join
	JoinServers map[int][]string
	// Leave
	LeaveGIDs []int
	// Move
	MoveShard int
	MoveGID int
	// Query
	QueryNum int
	QueryConfig Config
	
	ClientID int64
	SeqNum int64


}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// where the seq < maxseq
	sm.mu.Lock()
	_, isLeader := sm.rf.GetState()
	seq, ok := sm.clients[args.Id]
	if isLeader && ok && args.SeqNum <= seq {
		reply.Err = OK
		reply.WrongLeader = false
		DPrintf("Server %v replies client %v Join %v: success", sm.me, args.Id, args.SeqNum)
		sm.mu.Unlock()
		return 
	}

	sm.mu.Unlock()
	op := Op {
		ClientID: args.Id,
		SeqNum: args.SeqNum,
		Type: OpType_Join,
		JoinServers: args.Servers }
	
	
	index, _, isLeader := sm.rf.Start(op)
	if isLeader {
		sm.mu.Lock()
		resChan := make(chan Op, 1)
		sm.channels[index] = resChan
		sm.mu.Unlock()

		select {
			case <- resChan: {
				_, isLeader := sm.rf.GetState()
				if isLeader {
					reply.WrongLeader = false
					reply.Err = OK
				} else {
					reply.WrongLeader = true
					reply.Err = WrongLeader
				}
				DPrintf("Server %v replies Client %v Seq %v success directly ", sm.me, args.Id, args.SeqNum)
		
			}
			case <- time.After(time.Millisecond * 800): {
				reply.WrongLeader = true
				reply.Err = WrongLeader
				DPrintf("Server %v timeout Client %v Seq %v ", sm.me, args.Id, args.SeqNum)
			}
		}
	
		
		sm.mu.Lock()
		delete(sm.channels, index)
		sm.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}



	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	_, isLeader := sm.rf.GetState()
	seq, ok := sm.clients[args.Id]
	if isLeader && ok && args.SeqNum <= seq {
		reply.Err = OK
		reply.WrongLeader = false
		DPrintf("Server %v replies client %v Leave %v: success directly", sm.me, args.Id, args.SeqNum)
		sm.mu.Unlock()
		return 
	}

	sm.mu.Unlock()
	op := Op { 
		ClientID: args.Id,
		SeqNum: args.SeqNum,
		Type: OpType_Leave,
		LeaveGIDs: args.GIDs }
	
	index, _, isLeader := sm.rf.Start(op)
	if isLeader {
		sm.mu.Lock()
		resChan := make(chan Op, 1)
		sm.channels[index] = resChan
		sm.mu.Unlock()

		select {
			case <- resChan: {
				_, isLeader := sm.rf.GetState()
				if isLeader {
					reply.WrongLeader = false
					reply.Err = OK
				} else {
					reply.WrongLeader = true
					reply.Err = WrongLeader
				}
				DPrintf("Server %v replies Client %v Seq %v successfully ", sm.me, args.Id, args.SeqNum)
		
			}
			case <- time.After(time.Millisecond * 800): {
				reply.WrongLeader = true
				reply.Err = WrongLeader
				DPrintf("Server %v timeout Client %v Seq %v ", sm.me, args.Id, args.SeqNum)
			}
		}
	
		
		sm.mu.Lock()
		delete(sm.channels, index)
		sm.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op {ClientID: args.Id,
			SeqNum: args.SeqNum,
			Type: OpType_Move,
			MoveShard: args.Shard,
			MoveGID: args.GID}
	
	
	index, _, isLeader := sm.rf.Start(op)
	if isLeader {
		sm.mu.Lock()
		resChan := make(chan Op, 1)
		sm.channels[index] = resChan
		sm.mu.Unlock()

		select {
			case <- resChan: {
				_, isLeader := sm.rf.GetState()
				if isLeader {
					reply.WrongLeader = false
					reply.Err = OK
				} else {
					reply.WrongLeader = true
					reply.Err = WrongLeader
				}
				DPrintf("Server %v replies Client %v Seq %v successfully ", sm.me, args.Id, args.SeqNum)
		
			}
			case <- time.After(time.Millisecond * 800): {
				reply.WrongLeader = true
				reply.Err = WrongLeader
				DPrintf("Server %v timeout Client %v Seq %v ", sm.me, args.Id, args.SeqNum)
			}
		}
	
		
		sm.mu.Lock()
		delete(sm.channels, index)
		sm.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op {
		ClientID: args.Id,
		SeqNum: args.SeqNum,
		Type: OpType_Query,
		QueryNum: args.Num }

	index, _, isLeader := sm.rf.Start(op)
	if isLeader {
		sm.mu.Lock()
		resChan := make(chan Op, 1)
		sm.channels[index] = resChan
		sm.mu.Unlock()

		select {
			case op := <- resChan: {
				_, isLeader := sm.rf.GetState()
				if isLeader {
					
					reply.Config = op.QueryConfig
					reply.WrongLeader = false
					reply.Err = OK
				
				} else {
					reply.WrongLeader = true
					reply.Err = WrongLeader
				}
				DPrintf("Server %v replies Client %v Seq %v successfully ", sm.me, args.Id, args.SeqNum)
			}
			case <- time.After(time.Millisecond * 800): {
				reply.WrongLeader = true
				reply.Err = WrongLeader
				DPrintf("Server %v timeout Client %v Seq %v ", sm.me, args.Id, args.SeqNum)
			}
		}
	
		
		sm.mu.Lock()
		delete(sm.channels, index)
		sm.mu.Unlock()
	} else {
		reply.WrongLeader = true
		reply.Err = WrongLeader
	}

}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.killed = true
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) handleJoinOp (op *Op) {
	
}

func (sm *ShardMaster) handleLeaveOp (op *Op) {
	
}

func (sm *ShardMaster) handleMoveOp (op *Op) {

}

func (sm *ShardMaster) handleQueryOp (op *Op) {

}

func (sm *ShardMaster) applyLog() {
	for !sm.killed {
		msg := <- sm.applyCh
		op := msg.Command.(Op)

		sm.mu.Lock()
		
		maxSeq, ok := sm.clients[op.ClientID]
		if !ok || maxSeq < op.SeqNum {
			DPrintf("Server %v applied log at index %v.", sm.me, msg.CommandIndex)
			switch op.Type {
				case OpType_Join: {
					sm.handleJoinOp(&op)
				}
				case OpType_Leave: {
					sm.handleLeaveOp(&op)
				}
				case OpType_Move: {
					sm.handleMoveOp(&op)
				}
				case OpType_Query: {
					sm.handleQueryOp(&op)
				}
			}
			sm.clients[op.ClientID] = op.SeqNum
		
			ch, ok := sm.channels[msg.CommandIndex]
			
			delete(sm.channels, msg.CommandIndex)

			sm.mu.Unlock()
			
			_, isLeader := sm.rf.GetState()
			if ok && isLeader {
				ch <- op
			}
		} else {
			sm.mu.Unlock()
		}

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.clients = make(map[int64]int64)
	sm.channels = make(map[int]chan Op)

	// Your code here.
	go sm.applyLog()
	return sm
}
