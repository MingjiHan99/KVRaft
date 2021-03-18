package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import "sync"
import "sync/atomic"
import "../labrpc"
import "log"
import "time"
import "math/rand"

import "bytes"
import "../labgob"


const electionTimeout int = 200 
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	LastIncludedIndex int
}
type Role int32
const (
	Role_Leader Role = 0
	Role_Candidate Role = 1
	Role_Follower Role = 2
)

type Log struct  {
	Term int
	Index int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyChan chan ApplyMsg

	role Role
	lastHeartBeatTime time.Time
	lastElectionTime time.Time
	timeout int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// Persistent state
	currentTerm int
	votedFor int
	log []Log
	lastIncludedIndex int
	lastIncludedTerm int
	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leader

	nextIndex []int
	matchIndex []int
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Role_Leader

	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
	var logItems []Log
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logItems) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil {
		   log.Fatalf("Unable to read persisted state")
	   } else {
		   rf.currentTerm = currentTerm
		   rf.votedFor = votedFor
		   rf.log = logItems
		   rf.lastIncludedIndex = lastIncludedIndex
		   rf.lastIncludedTerm = lastIncludedTerm
		   // do not apply the log in the snapshot
		   rf.lastApplied = rf.lastIncludedIndex
	   }
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) readSnapshot() {

	msg := ApplyMsg {
		CommandValid: false,
		Command: rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex	}
	rf.applyChan <- msg

}

func (rf *Raft) GetSnapshot() ([]byte, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.persister.ReadSnapshot(), rf.lastIncludedIndex
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()	
	if args.Term > rf.currentTerm {
		rf.BecomeFollower(args.Term)
	} 
	reply.Term = rf.currentTerm

	if args.Term == rf.currentTerm {
		rf.lastHeartBeatTime = time.Now()
		if rf.role == Role_Candidate {
			rf.BecomeFollower(args.Term)
		}
		if args.LastIncludedIndex > rf.lastIncludedIndex {
			DPrintf("Follower %v get installSnapshot RPC at lastIncludedIndex %v lastApplied %v", rf.me, args.LastIncludedIndex,  rf.lastApplied)
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm
			// important
			rf.lastApplied = rf.lastIncludedIndex
			rf.logTruncate(args.LastIncludedIndex)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(rf.currentTerm)
			e.Encode(rf.votedFor)
			e.Encode(rf.log)
			e.Encode(rf.lastIncludedIndex)
			e.Encode(rf.lastIncludedTerm)
			state := w.Bytes()
			rf.persister.SaveStateAndSnapshot(state, args.Data)
			
	
			rf.readSnapshot()	
	
		}

	}

}

func (rf *Raft) getHeartBeatTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartBeatTime
}

func (rf *Raft) getElectionTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastElectionTime
}

func (rf *Raft) getRealLogIndex(i int) int {
	if len(rf.log) > 0 {
		startIndex := rf.log[0].Index 
		lastIndex, _ := rf.getLastLogInfo() // ok
	//	fmt.Printf("Start index: %v\n", startIndex)
		if startIndex <= i && i <= lastIndex {
			return i - startIndex
		}
	}
	return -1
}
// discard log before index (including index)
func (rf *Raft) logTruncate(index int) {
	if len(rf.log) == 0 {
		return 
	} else if len(rf.log) > 0 {
		lastLogIndex, _ := rf.getLastLogInfo()
		if index > lastLogIndex {
			rf.log = []Log{}
		} else {
			realIndex := rf.getRealLogIndex(index)
			rf.log = rf.log[realIndex + 1: ]
			DPrintf("Server %v truncate the log at index %v, realIndex: %v lastIndex: %v log len: %v", rf.me, index, realIndex, lastLogIndex, len(rf.log))
		
 		}
	}
}

func (rf *Raft) GenerateSnapshot(snapshot []byte, lastApplied int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastApplied > rf.lastIncludedIndex {
		DPrintf("Server %v generate snapshot at index lastApplied %v", rf.me, lastApplied)
		rf.lastIncludedIndex = lastApplied
		rf.lastIncludedTerm = rf.log[rf.getRealLogIndex(lastApplied)].Term
		rf.logTruncate(lastApplied)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		e.Encode(rf.lastIncludedIndex)
		e.Encode(rf.lastIncludedTerm)
		state := w.Bytes()
		rf.persister.SaveStateAndSnapshot(state, snapshot)
	} 
	
}
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}
func (rf *Raft) applyLog() {
	for !rf.killed() {

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			DPrintf("Applying log Instance %v: len %v commit index %v lastApplied %v", rf.me, rf.lastIncludedIndex + len(rf.log),  rf.commitIndex, rf.lastApplied )
			rf.lastApplied += 1
			realIndex := rf.getRealLogIndex(rf.lastApplied)
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.log[realIndex].Index,
				Command: rf.log[realIndex].Command	}
		
			rf.mu.Unlock()
			rf.applyChan <- msg
		} else {
			rf.mu.Unlock()
		}
		
	}
}

func (rf *Raft) mainLoop() {
	for !rf.killed() {
		
		timeout := getRandTimeout()
		time.Sleep(time.Millisecond * time.Duration(rf.timeout))
	
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		
		switch role {
			// leader does not have something needed to be check routinely
			// when the instance wins the election, the StartElection will open the heatbeat thread
			// using for loop to create many heatbeat threads is strange
			case Role_Candidate: {
				//DPrintf("Instance %v is the candidate now", rf.me)
				sub := time.Now().Sub(rf.getElectionTime()) 
				if sub > time.Duration(timeout) * time.Millisecond {
					rf.mu.Lock()
					rf.BecomeCandidate()
					rf.mu.Unlock()
					go rf.StartElection()
					DPrintf("Instance %v starts new election (candidate->candidate)", rf.me)
				}
			}
			case Role_Follower: {
			//	DPrintf("Instance %v is the follower now", rf.me)
				sub := time.Now().Sub(rf.getHeartBeatTime()) 
				if sub > time.Duration(timeout) * time.Millisecond {
					DPrintf("Instance %v starts election (follower->candidate)", rf.me)
					rf.mu.Lock()
					rf.BecomeCandidate()
					rf.mu.Unlock()
					go rf.StartElection()
				}
			}
		}
	}
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
	IsHeartBeat bool
}

type AppendEntriesReply struct {
	Term int
	Success bool
	XTerm int
	XIndex int
	XLen int
	NextIndex int	
}

func (rf *Raft) StartAppendEntries(is bool) {
	for !rf.killed() {
		
		rf.mu.Lock()
		// nextIndex >= 1
		if rf.role != Role_Leader {
			rf.mu.Unlock()
			return 
		}

		rf.mu.Unlock()
		

		DPrintf("Instance %v is the leader and sending entries\n", rf.me)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func (id int) {
					for !rf.killed() {
						rf.mu.Lock()
						// nextIndex >= 1
						if rf.role != Role_Leader {
							rf.mu.Unlock()
							return 
						}
						lastLogIndex, _ := rf.getLastLogInfo()
						if rf.nextIndex[id] >= rf.lastIncludedIndex + 1 {
							DPrintf("Server %v send log interval [%v, %v]", rf.me, rf.nextIndex[id], lastLogIndex)
							DPrintf("Server %v Last included index: %v ", rf.me, rf.lastIncludedIndex)
							prevLogIndex := rf.nextIndex[id] - 1
							prevLogTerm := 0
							if prevLogIndex > rf.lastIncludedIndex { // && prevLogIndex <= lastLogIndex {
								prevLogTerm = rf.log[rf.getRealLogIndex(prevLogIndex)].Term	
							} else if prevLogIndex == rf.lastIncludedIndex {
								prevLogTerm = rf.lastIncludedTerm
							}

							entries := []Log{}
						
							for j := rf.nextIndex[id]; j <= lastLogIndex; j++  {
								entries = append(entries, rf.log[rf.getRealLogIndex(j)])
							}

							rf.mu.Unlock()
							args := AppendEntriesArgs{
								Term: rf.currentTerm,
								LeaderId: rf.me,
								PrevLogTerm: prevLogTerm,
								PrevLogIndex: prevLogIndex,
								Entries: entries,
								LeaderCommit: rf.commitIndex,
								IsHeartBeat: is}

							reply := AppendEntriesReply{}

							ok := rf.sendAppendEntries(id, &args, &reply)
							if ok {
								rf.mu.Lock()
								if reply.Term > rf.currentTerm {

									rf.BecomeFollower(reply.Term)
									rf.mu.Unlock()
									return 
								} 
								// only update commitindex when success
								// otherwise the commitIndex may fall back
								if args.Term == rf.currentTerm && rf.role == Role_Leader {
									if reply.Success {

										rf.matchIndex[id] = prevLogIndex + len(entries)
										rf.nextIndex[id] = rf.matchIndex[id] + 1
										
										rf.matchIndex[rf.me] = rf.lastIncludedIndex + len(rf.log)
										nextCommitIdx := rf.commitIndex
										for i := 0; i < len(rf.peers); i++ {
											DPrintf("Leader %v Matchindex[%v]=%v",rf.me, i , rf.matchIndex[i])
										}

										for i := rf.commitIndex + 1; i <= rf.matchIndex[rf.me]; i++ {
											vote := 0
											for j := 0; j < len(rf.peers); j++ {
												if rf.matchIndex[j] >= i {
													vote += 1
												}	
											}

											if vote >= len(rf.peers) / 2 + 1  {
												realIndex := rf.getRealLogIndex(i)
												if realIndex != -1 && rf.log[realIndex].Term == rf.currentTerm || realIndex == rf.lastIncludedIndex && rf.currentTerm == rf.lastIncludedTerm {
													nextCommitIdx = i
												}
											} 
										}
										
										DPrintf("Leader %v commitIndex: %v", rf.me , nextCommitIdx)
										
										rf.commitIndex = nextCommitIdx
										rf.mu.Unlock()
										break
									} else {
										DPrintf("Before fall back : nextIndex[%v]= %v", id, rf.nextIndex[id])
										if reply.NextIndex != -1 {
											rf.nextIndex[id] = reply.NextIndex
										} else if reply.XLen != -1 {
											rf.nextIndex[id] = reply.XLen + 1
										} else {
											rf.nextIndex[id] = reply.XIndex
											for i := len(rf.log) - 1; i >= 0; i-- {
												if rf.log[i].Term < reply.XTerm {
													break
												}
												if rf.log[i].Term == reply.XTerm {
													rf.nextIndex[id] = rf.log[i].Index
													break
												}
											}					
										}
									
										DPrintf("After fall back : nextIndex[%v]= %v", id, rf.nextIndex[id])
									
									}
								} 

								rf.mu.Unlock()
							}
						} else { // send install snapshot RPC
							args := InstallSnapshotArgs{
								Term : rf.currentTerm,
								LeaderId: rf.me,
								Data:  rf.persister.ReadSnapshot(),
								LastIncludedIndex: rf.lastIncludedIndex,
								LastIncludedTerm: rf.lastIncludedTerm }
							reply := InstallSnapshotReply{

							}
							rf.mu.Unlock()
							ok := rf.sendInstallSnapshot(id, &args, &reply)
							rf.mu.Lock()
							if ok {							
								if reply.Term > rf.currentTerm {
									rf.BecomeFollower(reply.Term)
									rf.mu.Unlock()
									return 
								}
								if args.Term == rf.currentTerm && rf.role == Role_Leader {
									rf.nextIndex[id] = args.LastIncludedIndex + 1
									rf.matchIndex[id] = args.LastIncludedIndex
								}
							}
							rf.mu.Unlock()
						}
					}
					
				}(i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	// WRONG IMPLEMENTATION
	// if rf.role == rf.currentTerm then covert to follower 
	
	if args.Term > rf.currentTerm  {
		rf.BecomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XLen = -1
	reply.XTerm = -1
	reply.XIndex = -1
	reply.NextIndex = -1
	if args.Term == rf.currentTerm {	
		// only heartbeat with latest term is valid	
		rf.lastHeartBeatTime = time.Now()
		// CORRECT IMPLEMENTATION (found by Test)
		// When the leader's term is greater or equals to candidate's term
		// candidate can go to follower 
		// do not covert to follower when receive the heartbeat
		if rf.role == Role_Candidate {
			rf.BecomeFollower(args.Term)
		}

		lastLogIndex, _ := rf.getLastLogInfo()
		if args.PrevLogIndex <= lastLogIndex {
			
			realPrevLogIndex := rf.getRealLogIndex(args.PrevLogIndex)
			
			logTerm := 0
			if realPrevLogIndex != -1 {
				logTerm = rf.log[realPrevLogIndex].Term		
			} else if args.PrevLogIndex == rf.lastIncludedIndex {
				logTerm = rf.lastIncludedTerm
			} else if args.PrevLogIndex < rf.lastIncludedIndex {
				// impossible ?????
				reply.Success = false
				reply.NextIndex = rf.lastIncludedIndex + 1
				return 
			}

			if logTerm == args.PrevLogTerm {
				
				reply.Success = true
				for idx := 0; idx < len(args.Entries); idx++ {
					realIndex := rf.getRealLogIndex(args.Entries[idx].Index)
					// has conflict
					if realIndex != -1  {
						if args.Entries[idx].Term != rf.log[realIndex].Term {
							rf.log = append(rf.log[:realIndex], args.Entries[idx:]...)
							break	
						}
					} else { // no conflict or log does not exists
						rf.log = append(rf.log, args.Entries[idx])
					}
				}
				
				rf.persist()
				
				
				if args.LeaderCommit > rf.commitIndex {
					lastEntryIndex, _ := rf.getLastLogInfo() // ok 
					if args.LeaderCommit < lastEntryIndex {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = lastEntryIndex
					}
				}
			} else {
				reply.XTerm = logTerm
				index := -1
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == reply.XTerm {
						index = rf.log[i].Index
						break
					}
				}
				reply.XIndex = index
			}
		} else {
			reply.XLen = len(rf.log) + rf.lastIncludedIndex
		}
	}	



	DPrintf("HeartBeat: %v Instance %v receive rpc from %v. HeartBeat Term:%v My Term: %v Result: %v Entries size: %v Commit Index: %v Log Length: %v PreLogIndex: %v",
		args.IsHeartBeat ,rf.me, args.LeaderId, args.Term, rf.currentTerm, reply.Success, len(args.Entries), rf.commitIndex, rf.lastIncludedIndex + len(rf.log), args.PrevLogIndex)
	
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) getLastLogInfo() (int, int) {
	// use it with lock
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log) - 1].Index
		lastLogTerm = rf.log[len(rf.log) - 1].Term
	} else if len(rf.log) == 0 {
		lastLogIndex = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) StartElection() {
	
	rf.mu.Lock()
	if rf.role != Role_Candidate {
		rf.mu.Unlock()
		return 
	}
	// vote to itself
	voteCount := 1
	lastLogIndex, lastLogTerm := rf.getLastLogInfo() // ok
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm }

	rf.mu.Unlock()


	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(id int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(id, &args, &reply)
				
				if ok {
					DPrintf("Instance %v  gets vote reply from %v, result %v", rf.me, id, reply.VoteGranted)
						
					if reply.Term > args.Term {
						rf.mu.Lock()
						rf.BecomeFollower(reply.Term)
						rf.mu.Unlock()
						return 
					} 
					rf.mu.Lock()
					if rf.currentTerm == args.Term && rf.role == Role_Candidate {
						if reply.VoteGranted  {
							voteCount += 1
						}
						DPrintf("Instance %v  gets vote count: %v", rf.me, voteCount)
					
						if voteCount >= len(rf.peers) / 2 + 1 {
							DPrintf("Instance %d wins the election (candidate -> leader)", rf.me)
							rf.BecomeLeader()
							rf.mu.Unlock()
							go rf.StartAppendEntries(true)
							return
						}
						
					}	
					rf.mu.Unlock()
						
				}
			}(i)
		}
	}

}

func (rf *Raft) BecomeLeader() {
	rf.role = Role_Leader
	lastIndex := 0
	if len(rf.log) > 0 {
		lastIndex = rf.log[len(rf.log) - 1].Index
	} else if len(rf.log) == 0 {
		lastIndex = rf.lastIncludedIndex 
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) BecomeCandidate() {
	rf.timeout = getRandTimeout()
	rf.role = Role_Candidate
	rf.currentTerm += 1
	rf.lastElectionTime = time.Now() 
	rf.votedFor = rf.me
	rf.persist()
}


func (rf *Raft) BecomeFollower(term int) {
	rf.timeout = getRandTimeout()
	rf.lastHeartBeatTime = time.Now() // reset timeout!
	rf.role = Role_Follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Instance %v receive vote request from %v. Request Term:%v My Term: %v",
		rf.me, args.CandidateId, args.Term, rf.currentTerm)

	if args.Term > rf.currentTerm {
		rf.BecomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 ||  rf.votedFor == args.CandidateId {

		lastLogIndex, lastLogTerm := rf.getLastLogInfo() // ok
		// at least as up-to-date as receiver's log
		DPrintf("LastLogIndex %v LastLogTerm %v, my Index %v, my Term %v",
			args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm )
		if args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm || args.LastLogTerm > lastLogTerm {
			rf.lastHeartBeatTime = time.Now()	
			rf.votedFor = args.CandidateId	
			reply.VoteGranted = true
			rf.persist()
			DPrintf("Instance %v grants vote to %v", rf.me, rf.votedFor)
		}
	
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	
	term = rf.currentTerm
	isLeader = rf.role == Role_Leader
	
	rf.mu.Unlock()

	if isLeader {
		
		rf.mu.Lock()
		lastLogIndex, _ := rf.getLastLogInfo() //ok
		index = lastLogIndex + 1
		entry := Log{
			Term: rf.currentTerm,
			Index: index,
			Command: command}
		rf.log = append(rf.log, entry)
		rf.persist()
		DPrintf("Instance %v add new log %v %v ", rf.me, index, rf.currentTerm)
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Role_Follower
	rf.timeout = getRandTimeout()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartBeatTime = time.Now()
	rf.lastElectionTime = time.Now()
	rf.log = []Log{}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0 
	rf.lastApplied = 0

	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	/*
	f, err := os.Create(strconv.Itoa(rf.me))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	log.SetOutput(f)*/
	// initialize from state persisted before a crash*/
	DPrintf("Instance %v starts the main loop, timeout limit: %v", rf.me ,rf.timeout)
	rf.readPersist(persister.ReadRaftState())
//	rf.readSnapshot()
	go rf.mainLoop()
	go rf.applyLog()


	return rf
}


func getRandTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return electionTimeout + rand.Intn(electionTimeout)
}