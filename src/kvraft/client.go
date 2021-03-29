package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("Client Get %v", key)
	ck.mu.Lock()
	i := 0
	if ck.lastLeader != -1 {
		i = ck.lastLeader
		DPrintf("Client last leader is %v", i)
	} 
	ck.seqNumber += 1
	args := GetArgs {
		Key: key,
		Id: ck.id,
		SeqNum: ck.seqNumber}
	reply := GetReply{

	}
	ck.mu.Unlock()

	res := ""
	for {
		DPrintf("Client %v call %v when Get %v", ck.id, i, key)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				i = (i + 1) % len(ck.servers)
				DPrintf("Client %v retry another leader (WrongLeader) : %v when Get(%v)", ck.id, i, key)
				continue
			}

			if reply.Err == OK {
				ck.lastLeader = i
				res = reply.Value
				DPrintf("Client %v get value of key %v : %v",ck.id,  key, res)
			} 

		    if reply.Err == ErrNoKey{
				DPrintf("Client %v get empty key :%v", ck.id, key)
			}

			break 
		} else {
			i = (i + 1) % len(ck.servers)
			DPrintf("Client %v retry another leader (No Reply) : %v when Get(%v)", ck.id, i, key)
		}
	}
	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("Client %v (%v,%v)", op, key, value)
	ck.mu.Lock()
	i := 0
	if ck.lastLeader != -1 {
		i = ck.lastLeader
	} 
	ck.seqNumber += 1
	args := PutAppendArgs {
		Key: key,
		Value: value,
		Op: op,
		Id: ck.id,
		SeqNum: ck.seqNumber}
	reply := PutAppendReply{
	}
	ck.mu.Unlock()
	for {
		DPrintf("Client %v call %v when PutAppend %v, %v", ck.id, i, key, value)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				i = (i + 1) % len(ck.servers)
				DPrintf("Client %v retry (WrongLeader) Op %v(%v, %v)", ck.id, op, key, value)
				continue
			}

			if reply.Err == OK {
				ck.lastLeader = i
				DPrintf("Client %v PutAppend Success(%v, %v)",  ck.id, key, value)
			}

			// there is no nokey error for putappend method
			break

		}  else {
			i = (i + 1) % len(ck.servers)
			DPrintf("Client %v retry Op (NoReply) %v(%v, %v)", ck.id, op, key, value)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
