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

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	elecTimerCh chan bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent storage (updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (0 init)
	votedFor    int        //candidateID that recieved vote in current term (or null if none)
	log         []LogEntry //each entry contains command for state machineA
	status      int        //status of raft node
	//volatile (not persisted on disk)
	//
	commitIndex int //index of highest log entry known to be committed (init to 0)
	lastApplied int //index of highest log entry applied to state machine (init to 0)

	//volatile state on leaders (reinitialized after election)
	nextIndex  []int //for each serverindex of next log entry to send to server (init to leader last log index + 1)
	matchIndex []int //for each server,index of highest log entry known to be replicated on server (init 0)

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

// Max returns the larger of x or y.
func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

//handler for appendEntries RPC request
// leader should send out heartbeats periodically, resetting election timer
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	
	if rf.status == Candidate || rf.status == Follower {
		rf.status = Follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		reply.Term = Max((rf.currentTerm), (args.Term))
		fmt.Println("signalling on elecTimerCh for ", rf.me)
		//reset election timer
		rf.elecTimerCh <- true
		return
	}
	if rf.status == Leader {
		if args.Term > rf.currentTerm {
			rf.status = Follower
			reply.Term = args.Term
			reply.Success = true
			rf.currentTerm = args.Term
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
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func getNewElectionTimer() time.Duration {
	return time.Duration(rand.Intn(600-400+1)+400) * time.Millisecond
}

func (rf *Raft) resetElectionTimerAndSleep(duration time.Duration) {
	select {
	//TODO: calling resetElectionTimer could cause stackOverflow indefinetly,
	// add shouldReset and call that returned back in ticker
	case <-rf.elecTimerCh:
		//fmt.Printf("i, %v, recieved heartbeat, resetting election timer\n", rf.me)
		rf.resetElectionTimerAndSleep(getNewElectionTimer())
	case <-time.After(duration):
		fmt.Printf("Election timer expired, i, %v, becoming candidate!\n", rf.me)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status == Leader {
			//leader should send our empty heartbeat appendEntries to peers
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				//TODO: Implement what is actually prevLogIndex? in 2b
				args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm: rf.log[len(rf.log) - 1].Term, LeaderCommit: rf.commitIndex}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					fmt.Println("error sending append entries to peer:", i)
					continue
				}

			}
			continue //continue sending heartbeats as leader
		}
		rf.mu.Unlock()

		//TODO: solve data race
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == Follower {
			electionTimeout := getNewElectionTimer()
			//to randomize sleeping time using
			// time.Sleep().
			rf.resetElectionTimerAndSleep(electionTimeout)
		}
		
		// Your code here to check if a leader election should
		// be started after waking up
		// become a candidate and start election
		rf.mu.Lock()
		rf.status = Candidate
		voteCount := 1      //vote for self
		rf.currentTerm += 1 //increase term counter
		rf.mu.Unlock()
		//send out request vote to all peers (skipping sending one to ourself)

		majorityPoint := len(rf.peers) / 2

		for i := range rf.peers {
			if i != rf.me {
				//TODO lastLogIndex is not commitIndex, i think
				args := &RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: rf.commitIndex,
					LastLogTerm: rf.log[len(rf.log) - 1].Term}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				if !ok {
					fmt.Printf("error with sending vote request, server %v may be down\n", i)
					//skip requesting vote from that server this iteration
					continue
				}
				responseTerm := reply.Term
				if responseTerm > rf.currentTerm {
					// peer is bigger than me, the candidate, i submit to it as a follower!
					rf.currentTerm = responseTerm
					rf.mu.Lock()
					rf.status = Follower
					rf.mu.Unlock()
					rf.votedFor = args.CandidateID
					fmt.Println("SUBMITTING TO ANOTHER FOLLOWER AS CANDIDATE!")
					break
				}
				responseVoted := reply.VoteGranted

				if responseVoted {
					voteCount += 1
				}

			}
		}
		fmt.Println("vote count:", voteCount)
		fmt.Println("length: ", len(rf.peers))

		if rf.status == Candidate && voteCount > majorityPoint {
			rf.status = Leader
			fmt.Println("NEW LEADER")
		}
	}
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//yes i already voted for you bro
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateID {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		fmt.Println("here'")
	}

	//bro i have a larger term than you(or bro i already voted for someone with same term),
	//no way im voting for you
	if args.Term < rf.currentTerm || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm		
		fmt.Println("here'")

		return
	}
	fmt.Printf("candidate term and current term: %v, %v \n", args.Term, rf.currentTerm)

	//sure bro ill vote for you since your term is larger
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	//if candidates log is at least as up to date as recievers logs, grant vote
	// checks followers logs are ahead from candidates logs, if it is
	//ahead, return false to the candidate

	if rf.commitIndex >= 0 {
		lastRecieverTerm := rf.log[rf.commitIndex].Term
		if lastRecieverTerm > args.LastLogTerm ||
			(lastRecieverTerm == args.LastLogTerm && rf.commitIndex > args.LastLogIndex) {
			//vote no bro i have a bigger term than you (or same term but bigger index)
			reply.VoteGranted = false
			reply.Term = lastRecieverTerm
			return
		}
		//log with the greater term is more up to date
	}
	fmt.Println("Voting for, ", args.CandidateID)
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.status = Follower
	rf.persist()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int //candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //current term for candidate to update itself
	VoteGranted bool //true means candidate recieved vote!
}

type LogEntry struct {
	Term    int
	command interface{}
}

//used for heartbeat and invoked by leader to replicate log entries
type AppendEntriesArgs struct {
	Term         int // leaders term
	LeaderID     int // so follower can redirect clients to me, the leader sending this
	PrevLogIndex int //index of log entry preceding new ones
	PrevLogTerm  int //term of prevLogIndex entry

	//entries int[] log entries to store

	LeaderCommit int //leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

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
	rf.currentTerm = 0
	rf.me = me
	rf.elecTimerCh = make(chan bool)
	rf.status = Follower

	rf.log = []LogEntry{
		{
			command: nil,
			Term:    0,
		},
	}
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
