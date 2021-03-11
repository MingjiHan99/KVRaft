package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader int
	seqNumber int64
	id int64
	mu sync.Mutex
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
	ck.lastLeader = -1
	ck.seqNumber = 0
	ck.id = time.Now().UnixNano()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seqNumber += 1
	args.Num = num
	args.SeqNum = ck.seqNumber
	args.Id = ck.id
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seqNumber += 1
	args.Servers = servers
	args.SeqNum = ck.seqNumber
	args.Id = ck.id
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seqNumber += 1
	args.GIDs = gids
	args.SeqNum = ck.seqNumber
	args.Id = ck.id
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seqNumber += 1
	args.Shard = shard
	args.GID = gid
	args.SeqNum = ck.seqNumber
	args.Id = ck.id
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
