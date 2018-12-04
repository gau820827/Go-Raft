package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

type AppendResponse struct {
	ret  *pb.AppendEntriesRet
	err  error
	peer string
}

type VoteResponse struct {
	ret  *pb.RequestVoteRet
	err  error
	peer string
}

// Structure of a Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput

	appendResponseChan chan AppendResponse
	voteResponseChan   chan VoteResponse

	me          string
	peerClients map[string]pb.RaftClient
	lastLead    string

	// state a Raft server must maintain.
	State    int
	CommitCh chan bool

	VoteCount int

	// Volatile State on leaders:
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// Persistant State on all servers:
	CurrentTerm int64
	VoteFor     string
	Log         []*pb.Entry
	LogResponse []chan pb.Result

	// Volatile State on all servers:
	CommitIndex int64
	LastApplied int64

	// Conatact point to the KVstore service
	s *KVStore
}

// Some functionalities
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (raft *Raft) GetIndex(i int64) int64 {
	return raft.Log[i].Index
}

func (raft *Raft) GetTerm(i int64) int64 {
	return raft.Log[i].Term
}
func (raft *Raft) GetFirstLogTerm() int64 {
	return raft.GetTerm(0)
}

func (raft *Raft) GetLasLogTerm() int64 {
	return raft.GetTerm(int64(len(raft.Log)) - 1)
}

func (raft *Raft) GetFirstIndex() int64 {
	return raft.GetIndex(0)
}

func (raft *Raft) GetLastLogIndex() int64 {
	return raft.GetIndex(int64(len(raft.Log)) - 1)
}

// Make an AppendEntries Call
func (raft *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	log.Printf("%s called AppendEntries on %s", arg.LeaderID, raft.me)
	raft.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Make a RequestVote Call
func (raft *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	log.Printf("%s called RequestVote on %s", arg.CandidateID, raft.me)
	raft.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// The handler for request vote
func (raft *Raft) HandleRequestVote(args *pb.RequestVoteArgs, ret *pb.RequestVoteRet) {

	// If current term is larger, update the ret to the current term and return
	if args.Term < raft.CurrentTerm {
		ret.Term = raft.CurrentTerm
		ret.VoteGranted = false
		return
	}

	// Become a follower
	if args.Term > raft.CurrentTerm {
		raft.CurrentTerm = args.Term
		raft.State = 0
		raft.VoteFor = ""
	}
	ret.Term = raft.CurrentTerm

	/*
					Election restriction
		     	If the logs have last entries with different terms,
		     	then the log with the later term is more up-to-date.
		     	If the logs end with the same term, then whichever log is longer is more up-to-date.
	*/
	upDated := (args.LasLogTerm > raft.GetLasLogTerm()) ||
		(args.LasLogTerm == raft.GetLasLogTerm() && args.LastLogIndex >= raft.GetLastLogIndex())

	fmt.Printf("Judgement: votefor %s, candidateID %s, update %v\n", raft.VoteFor, args.CandidateID, upDated)
	if (raft.VoteFor == "" || raft.VoteFor == args.CandidateID) && upDated {
		raft.State = 0
		raft.VoteFor = args.CandidateID
		ret.VoteGranted = true
		fmt.Printf("Server %s VoteReplies to cand %s +\n", raft.me, args.CandidateID)
	} else {
		fmt.Printf("Server %s VoteReplies to cand %s -\n", raft.me, args.CandidateID)
		ret.VoteGranted = false
	}
	return
}

// Requst vote for all other peers
func (raft *Raft) broadcastVote() {
	// Make the RequestVoteArgs
	args := &pb.RequestVoteArgs{}
	args.Term = raft.CurrentTerm
	args.CandidateID = raft.me
	args.LasLogTerm = raft.GetLasLogTerm()
	args.LastLogIndex = raft.GetLastLogIndex()
	for p, c := range raft.peerClients {
		// Send in parallel so we don't wait for each client.
		go func(c pb.RaftClient, p string, args *pb.RequestVoteArgs) {
			log.Printf("Server %s send RequestVote to %s\n", raft.me, p)
			ret, err := c.RequestVote(context.Background(), args)
			raft.voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
		}(c, p, args)
	}
}

// The handler for append entries
func (raft *Raft) HandleAppendEntries(args *pb.AppendEntriesArgs, ret *pb.AppendEntriesRet) {

	log.Printf("Server %s receive AppendEntries from Leader %s\n", raft.me, args.LeaderID)
	log.Printf("Term: Server / Leader = %d/ %d\n", raft.CurrentTerm, args.Term)

	// ret false if term < currentTerm
	ret.Success = false
	if args.Term < raft.CurrentTerm {
		ret.Term = raft.CurrentTerm
		return
	}

	// Become a follower if receiving AppendEntries from a larger term
	if args.Term > raft.CurrentTerm {
		raft.CurrentTerm = args.Term
		raft.State = 0
		raft.VoteFor = ""
	}

	ret.Term = args.Term

	// ret false if the PrevLogIndex sent by the leader doesn't match the LastLogIndex
	if args.PrevLogIndex > raft.GetLastLogIndex() {
		return
	}
	baseIndex := raft.GetFirstIndex()

	// ret false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= baseIndex {
		term := raft.GetTerm(args.PrevLogIndex - baseIndex)
		if args.PrevLogTerm != term {
			return
		} else {
			// Append the log successfully
			raft.Log = raft.Log[:args.PrevLogIndex+1-baseIndex]
			raft.Log = append(raft.Log, args.Entries...)
			ret.Success = true
		}
	}

	// If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > raft.CommitIndex {
		raft.CommitIndex = min(args.LeaderCommit, raft.GetLastLogIndex())
		raft.CommitCh <- true
	}
}

// If there exists an N such that N > CommitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set CommitIndex = N
func (raft *Raft) updateCommit() {
	for i := raft.CommitIndex + 1; i < int64(len(raft.Log)); i++ {
		// Check if there exists an N > CommitIndex
		count := 0

		// Counting replicas
		for p, _ := range raft.peerClients {
			if raft.matchIndex[p] >= i {
				count++
			}
		}
		if count > len(raft.peerClients)/2 && raft.GetTerm(i) == raft.CurrentTerm {
			raft.CommitIndex = i
		} else {
			break
		}
	}

	// If CommitIndex is updated
	if raft.CommitIndex == raft.GetLastLogIndex() {
		raft.CommitCh <- true
	}
}

// For all servers, if CommitIndex > lastApplied, increment lastApplied,
// and apply log[lastApplied] to the state machine
// The function to handle when it's available to update the commit log
func (raft *Raft) applyCommit() {
	CommitIndex := raft.CommitIndex
	lastApp := raft.LastApplied
	baseIndex := raft.GetFirstIndex()
	// store all new commited command
	// log.Printf("command index %d at %d, lastapplied: %d, log length: %d, offset: %d, log: %v\n",
	//    raft.CommitIndex, raft.me, raft.LastApplied, len(raft.Log), raft.getIndex(0), raft.Log)

	for {
		select {
		case ok := <-raft.CommitCh:
			if ok {
				print("Checkcommit!\n")
				left_res := int64(len(raft.Log) - len(raft.LogResponse))
				for i := lastApp + 1; i <= CommitIndex; i++ {
					print("Check %d\n", i)
					if i >= left_res {
						// There's something need to reply
						cmd := InputChannelType{*raft.Log[i-baseIndex].Cmd, raft.LogResponse[i-left_res]}
						raft.s.HandleCommand(cmd)
						raft.LogResponse = raft.LogResponse[1:]
					} else {
						// dummy apply
						print("Check %d\n", i)
						dummy := make(chan pb.Result)
						cmd := InputChannelType{*raft.Log[i-baseIndex].Cmd, dummy}
						go raft.s.HandleCommand(cmd)
						_ = <-dummy
						close(dummy)

					}
				}
				raft.LastApplied = raft.CommitIndex
			}
		}
	}
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand, min int, max int) time.Duration {
	// Constant
	var DurationMax = max
	var DurationMin = min
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand, min int, max int) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r, min, max))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}

	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	// Initialize the Raft server parameters
	raft.me = id
	raft.State = 0
	raft.VoteFor = ""
	raft.Log = append(raft.Log, &pb.Entry{Term: 0})
	raft.CurrentTerm = 0
	raft.CommitCh = make(chan bool)
	raft.peerClients = peerClients
	raft.s = s

	raft.appendResponseChan = make(chan AppendResponse)
	raft.voteResponseChan = make(chan VoteResponse)

	// Create a timer and start running it
	election_timer := time.NewTimer(randomDuration(r, 1000, 3000))
	heart_beat_timer := time.NewTimer(randomDuration(r, 100, 300))

	// A routine for handling the committed entry and respond
	go raft.applyCommit()

	// Run forever handling inputs from various channels
	for {

		select {
		case <-heart_beat_timer.C:
			log.Printf("Heart Beat Timeout")

			if raft.State == 2 {
				log.Printf("Send Heart Beat for Term %d", raft.CurrentTerm)
				// A leader should broadcast heartBeat after a period
				// Don't append anything to the log
				for p, c := range raft.peerClients {
					// A leader will broadcast the beacons
					// Send the AppendEntriesRequest
					baseIndex := raft.GetFirstIndex()

					// Preparing sending AppendEntries
					args := &pb.AppendEntriesArgs{}
					args.Term = raft.CurrentTerm
					args.LeaderID = raft.me
					args.PrevLogIndex = raft.nextIndex[p] - 1
					args.PrevLogTerm = raft.GetTerm(args.PrevLogIndex - baseIndex)
					args.Entries = make([]*pb.Entry, int64(len(raft.Log))-raft.nextIndex[p]+baseIndex)
					copy(args.Entries, raft.Log[args.PrevLogIndex+1-baseIndex:])
					args.LeaderCommit = raft.CommitIndex

					// Make the RPC call
					go func(c pb.RaftClient, p string, args *pb.AppendEntriesArgs) {
						log.Printf("Server %s send AppendEntries to %s\n", raft.me, p)
						ret, err := c.AppendEntries(context.Background(), args)
						raft.appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
					}(c, p, args)
				}

			}
			restartTimer(heart_beat_timer, r, 100, 300)

		case <-election_timer.C:
			// The timer went off.
			log.Printf("Election Timeout")

			// If not a leader, start an election
			if raft.State != 2 {
				log.Printf("Start Election for Term %d", raft.CurrentTerm+1)

				// Timeout, Start Election, vote for itself
				raft.State = 1
				raft.CurrentTerm++
				raft.VoteFor = raft.me

				// Request Vote for all peers
				// Make the RequestVoteArgs
				args := &pb.RequestVoteArgs{}
				args.Term = raft.CurrentTerm
				args.CandidateID = raft.me
				args.LasLogTerm = raft.GetLasLogTerm()
				args.LastLogIndex = raft.GetLastLogIndex()
				for p, c := range raft.peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string, args *pb.RequestVoteArgs) {
						log.Printf("Server %s send RequestVote to %s\n", raft.me, p)
						ret, err := c.RequestVote(context.Background(), args)
						raft.voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
					}(c, p, args)
				}
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(election_timer, r, 1000, 3000)

		case op := <-s.C:
			// We received an operation from a client

			// Check if it's a leader
			isLeader := raft.State == 2

			// We would like to append it to the log
			if isLeader {
				term := raft.CurrentTerm
				index := raft.GetLastLogIndex() + 1
				raft.Log = append(raft.Log, &pb.Entry{Term: term, Index: index, Cmd: &op.command})

				// BroadCasting the log after the new operation
				for p, c := range raft.peerClients {
					// A leader will broadcast the beacons
					// Send the AppendEntriesRequest
					baseIndex := raft.GetFirstIndex()

					// Preparing sending AppendEntries
					args := &pb.AppendEntriesArgs{}
					args.Term = raft.CurrentTerm
					args.LeaderID = raft.me
					args.PrevLogIndex = raft.nextIndex[p] - 1
					args.PrevLogTerm = raft.GetTerm(args.PrevLogIndex - baseIndex)
					args.Entries = make([]*pb.Entry, int64(len(raft.Log))-raft.nextIndex[p]+baseIndex)
					copy(args.Entries, raft.Log[args.PrevLogIndex+1-baseIndex:])
					args.LeaderCommit = raft.CommitIndex

					// Make the RPC call
					go func(c pb.RaftClient, p string, args *pb.AppendEntriesArgs) {
						log.Printf("Server %s send AppendEntries to %s\n", raft.me, p)
						ret, err := c.AppendEntries(context.Background(), args)
						raft.appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
					}(c, p, args)
				}

				// We want to preserve the response channel here, so that after committed successfully
				// the server would be able to send back the response to the client
				raft.LogResponse = append(raft.LogResponse, op.response)

			} else {
				// Not the leader, redirect the clients
				redirect := pb.Result{Result: &pb.Result_Redirect{&pb.Redirect{Server: raft.lastLead}}}
				op.response <- redirect
			}

		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peers
			log.Printf("Received append entry from %v", ae.arg.LeaderID)

			var ret pb.AppendEntriesRet
			raft.HandleAppendEntries(ae.arg, &ret)

			// Update the leaderID if successful
			// Which also means it becomes a follower
			if ret.Success {
				raft.State = 0
				raft.VoteFor = ""
				raft.lastLead = ae.arg.LeaderID
			}

			log.Printf("Send back the appendEntrieRet Term: %d Suc: %v\n", ret.Term, ret.Success)
			ae.response <- ret

			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(election_timer, r, 1000, 3000)

		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			log.Printf("Received vote request from %v\n", vr.arg.CandidateID)

			var ret pb.RequestVoteRet
			raft.HandleRequestVote(vr.arg, &ret)

			vr.response <- ret

		case vr := <-raft.voteResponseChan:
			// We received a response to a previous vote request.

			if vr.err != nil {
				// Do not do Fatal here since the peer might be gone but we should survive.
				log.Printf("VoteResponse: Error calling RPC %v", vr.err)
			} else {
				// Get a valid response
				log.Printf("Got response to vote request from %v", vr.peer)
				log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
				if vr.ret.VoteGranted == true {
					raft.VoteCount += 1
					if raft.State == 1 && raft.VoteCount > len(raft.peerClients)/2 {
						// A new leader is born
						// Initialize the nextIndex and  atchIndex
						fmt.Printf("%s wins the Election for term %d\n", raft.me, raft.CurrentTerm)
						raft.State = 2
						raft.nextIndex = make(map[string]int64, len(raft.peerClients))
						raft.matchIndex = make(map[string]int64, len(raft.peerClients))

						for p, _ := range raft.peerClients {
							raft.nextIndex[p] = raft.GetLastLogIndex() + 1
							raft.matchIndex[p] = 0
						}

						// BroadCast after winning the election
						log.Printf("Ready to broadcast the message with State %d", raft.State)
						s := raft.State
						for p, c := range raft.peerClients {
							if s == 2 {
								// A leader will broadcast the beacons
								// Send the AppendEntriesRequest
								baseIndex := raft.GetFirstIndex()

								// Preparing sending AppendEntries
								args := &pb.AppendEntriesArgs{}
								args.Term = raft.CurrentTerm
								args.LeaderID = raft.me
								args.PrevLogIndex = raft.nextIndex[p] - 1
								args.PrevLogTerm = raft.GetTerm(args.PrevLogIndex - baseIndex)
								args.Entries = make([]*pb.Entry, int64(len(raft.Log))-raft.nextIndex[p]+baseIndex)
								copy(args.Entries, raft.Log[args.PrevLogIndex+1-baseIndex:])
								args.LeaderCommit = raft.CommitIndex

								// Make the RPC call
								go func(c pb.RaftClient, p string, args *pb.AppendEntriesArgs) {
									log.Printf("Server %s send AppendEntries to %s\n", raft.me, p)
									ret, err := c.AppendEntries(context.Background(), args)
									raft.appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
								}(c, p, args)
							}
						}
					}
				} else if vr.ret.Term > raft.CurrentTerm {
					raft.CurrentTerm = vr.ret.Term
					raft.VoteCount = -1
					raft.State = 0
				}
			}

		case ar := <-raft.appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatal here since the peer might be gone but we should survive.
				log.Printf("appendResponse: Error calling RPC %v", ar.err)
			} else {

				// Ignore the response if the node is not a leader anymore
				if raft.State != 2 {
					continue
				}

				// if larger term -> follower
				if ar.ret.Term > raft.CurrentTerm {
					raft.CurrentTerm = ar.ret.Term
					raft.State = 0
					raft.VoteFor = ""
				} else if ar.ret.Success {
					// Update the matchIndex
					if len(raft.Log) > 0 {
						raft.nextIndex[ar.peer] = raft.Log[len(raft.Log)-1].Index + 1
						raft.matchIndex[ar.peer] = raft.nextIndex[ar.peer] - 1
					}

					// Check if there's something could be committed
					raft.updateCommit()

				} else {
					// failure, but need to update nextindex, decrement nextIndex and retry
					raft.nextIndex[ar.peer] -= 1
					for p, c := range raft.peerClients {
						if p == ar.peer {
							// A leader will broadcast the beacons
							// Send the AppendEntriesRequest
							baseIndex := raft.GetFirstIndex()

							// Preparing sending AppendEntries
							args := &pb.AppendEntriesArgs{}
							args.Term = raft.CurrentTerm
							args.LeaderID = raft.me
							args.PrevLogIndex = raft.nextIndex[p] - 1
							args.PrevLogTerm = raft.GetTerm(args.PrevLogIndex - baseIndex)
							args.Entries = make([]*pb.Entry, int64(len(raft.Log))-raft.nextIndex[p]+baseIndex)
							copy(args.Entries, raft.Log[args.PrevLogIndex+1-baseIndex:])
							args.LeaderCommit = raft.CommitIndex

							// Make the RPC call
							go func(c pb.RaftClient, p string, args *pb.AppendEntriesArgs) {
								log.Printf("Server %s send AppendEntries to %s\n", raft.me, p)
								ret, err := c.AppendEntries(context.Background(), args)
								raft.appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
							}(c, p, args)
						}
					}

				}
				log.Printf("Got append entries response from %v", ar.peer)
			}
		}
	}
	log.Printf("Strange to arrive here")
}
