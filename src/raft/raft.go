package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)


const (
	Follower = iota
	Candidate
	Leader
)




type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	applyCh   chan ApplyMsg
	currentTerm int
	votedFor    int
	logEntries  []Logs
	nextIndex    []int
	matchIndex   []int
	state        int
	lastActivity time.Time
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type Logs struct {
	Command interface{}
	Term    int
}


func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) persist() {
	
}

func (rf *Raft) readPersist(data []byte) {
	
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	rf.lastActivity = time.Now()
	rf.votedFor = args.CandidateId
	reply.Term = args.Term
	reply.VoteGranted = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) HandleLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastActivity = time.Now()
	rf.currentTerm = args.Term
	rf.state = Follower
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendLogRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.HandleLog", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	


	return index, term, isLeader
}

func (rf *Raft) Kill() {
	
}

func (rf *Raft) resetElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) Follower() {
	for rf.state == Follower {
		timeout := rf.resetElectionTimeout()
		rf.mu.Lock()
		lastActivity := rf.lastActivity
		rf.mu.Unlock()

		time.Sleep(timeout - time.Since(lastActivity))
		rf.mu.Lock()
		if time.Since(rf.lastActivity) >= timeout && rf.state == Follower {
			rf.state = Candidate
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Candidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastActivity = time.Now()
	votesGranted := 1
	totalPeers := len(rf.peers)
	majority := totalPeers/2 + 1
	term := rf.currentTerm
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := rf.logEntries[lastLogIndex].Term
	rf.mu.Unlock()

	voteCh := make(chan bool, totalPeers)

	for peer := range rf.peers {
		if peer != rf.me {
			go func(peerID int) {
				args := RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peerID, &args, &reply) {
					voteCh <- reply.VoteGranted
				} else {
					voteCh <- false
				}
			}(peer)
		}
	}

	timeout := rf.resetElectionTimeout()
	timer := time.NewTimer(timeout)

	for votesGranted < majority && rf.state == Candidate {
		select {
		case vote := <-voteCh:
			if vote {
				votesGranted++
			}
		case <-timer.C:
			rf.mu.Lock()
			rf.state = Follower
			rf.mu.Unlock()
		}
	}

	if votesGranted >= majority {
		rf.mu.Lock()
		rf.state = Leader
		rf.mu.Unlock()
	}
}

func (rf *Raft) Leader() {
	for rf.state == Leader {
		rf.mu.Lock()
		term := rf.currentTerm
		myID := rf.me
		rf.lastActivity = time.Now()
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer != myID {
				go func(peerID int) {
					args := AppendEntriesArgs{Term: term, LeaderId: myID}
					reply := AppendEntriesReply{}
					if rf.sendLogRequest(peerID, &args, &reply) {
						rf.mu.Lock()
						if reply.Term > term {
							rf.currentTerm = reply.Term
							rf.state = Follower
						}
						rf.mu.Unlock()
					}
				}(peer)
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, id int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:        peers,
		persister:    persister,
		me:           id,
		applyCh:      applyCh,
		logEntries:   []Logs{{Term: 0}},
		votedFor:     -1,
		state:        Follower,
		lastActivity: time.Now(),
	}

	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case Follower:
				rf.Follower()
			case Candidate:
				rf.Candidate()
			case Leader:
				rf.Leader()
			}
		}
	}()

	return rf
}
