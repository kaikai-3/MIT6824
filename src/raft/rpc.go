package raft
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int 
	CandidateId int
	LastLogIndex int 
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int 
	VoteGranted bool
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int 
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

// generate RequestVote args
func (rf *Raft)genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs{
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.logs[prevLogIndex -firstLogIndex+ 1 :]))
	copy(entries, rf.logs[prevLogIndex - firstLogIndex + 1:])

	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.logs[prevLogIndex - firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries: entries,
	}
	return args
}

//generate AppendEntries args
func (rf *Raft)genRequestVoteArgs() *RequestVoteArgs{
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm: rf.getLastLog().Term,
	}
	return args
}


// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf(`{Node %v}'s state is {state %vm ,term %v} after processing RequestVoteArgs,
	RequestVoteArgs %v and RequestVoteReply %v`,rf.me,rf.state,rf.currentTerm,args,reply)
	//request Term less than this node term
	//request Term equal to this node term but had voted for other node.
	//both cases above,refuse to vote
	if args.Term < rf.currentTerm ||
	(args.Term == rf.currentTerm  && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
			reply.Term, reply.VoteGranted = rf.currentTerm, false
			return
		}
	//may this node have a older state,and receive a heigher term ,so change state to follower
	if args.Term > rf.currentTerm{
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	//如果candidate的日志已经过时，拒绝投票
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm){
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted =rf.currentTerm, true	
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//deal with AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf(`{Node %v}'s state is {state %vm ,term %v} after processing 
	AppendEntriesArgs%v, and AppendEntriesReply %v`, rf.me, rf.currentTerm, rf.state,args,reply)
	//收到的term小于当前的term，拒绝投票
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//收到的term大于当前的term，更新当前的term
	if args.Term > rf.currentTerm{
		rf.currentTerm , rf.votedFor = args.Term,-1
	}

	//may this node have a older state,and receive a heigher term ,so change state to follower
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm(§5.3)
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// 检查日志是否匹配， 如果不匹配则返回这个conflict信息
	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastLog().Index
		// find the first index of the conflicting term
		if lastLogIndex < args.PrevLogIndex {
			// the last log index is smaller than the prevLogIndex, then the conflict index is the last log index
			reply.ConflictIndex, reply.ConflictTerm = lastLogIndex+1, -1
		} else {
			firstLogIndex := rf.getFirstLog().Index
			// find the first index of the conflicting term
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			reply.ConflictIndex, reply.ConflictTerm = index+1, args.PrevLogTerm
		}
		return
	}

	firstLogIndex := rf.getFirstLog().Index
	for index , entry := range args.Entries{
		if entry.Index - firstLogIndex >= len(rf.logs) || rf.logs[entry.Index - firstLogIndex].Term != entry.Term{
			rf.logs = append(rf.logs[:entry.Index - firstLogIndex], args.Entries[index:]...)
			break
		}
	}

	// 如果Lead提交的日志比当前的commitIndex大， 更新commitIndex
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex{
		DPrintf(`{node %v} advances commitIndex from %v to %v with leaderCommit %v in t
		erm %v`, rf.me , rf.commitIndex,newCommitIndex,args.LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}