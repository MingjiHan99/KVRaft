package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	Deleting       = "Deleting"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     int64
	SeqNum int64
	CfgNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id     int64
	SeqNum int64
	CfgNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type GetMigrationArgs struct {
	Num   int // configuration number
	Shard int // shard number
}

type GetMigrationReply struct {
	Num   int // configuration number
	Shard int // shard number
	Data  map[string]string
	Seq   map[int64]int64
	Err   Err
}

type GarbageCollectionArgs struct {
	Num   int // configuration number
	Shard int // shard number
}

type GarbageCollectionReply struct {
	Err Err // OK or Wrong Group
}
