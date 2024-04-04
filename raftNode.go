package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type VoteArguments struct {
	Term        int
	CandidateID int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

// What are these two structs
type AppendEntryArgument struct {
	Term         int
	LeaderID     int
	Entries      LogEntry
	PrevLogEntry LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

type LogEntry struct {
	Index int
	Term  int
}

type RaftNode struct {
	selfID          int
	serverNodes     []ServerConnection
	currentTerm     int
	votedFor        int
	state           string // "follower", "candidate", "leader"
	Mutex           sync.Mutex
	electionTimeout *time.Timer
	commitIndex     int
	log             []LogEntry
	nextIndex       map[int]int // nodeID: nodeIndex
	matchIndex      map[int]int //use this for leader to know when majority of servers have replicated an entry

	//log can be a key value pair where key is log entry number and pair is the log entry
	// {1: "log1", 2: "log2"}
}

// This function is designed to emulate a client reaching out to the
// server. Note that many of the realistic details are removed, for simplicity
func (node *RaftNode) ClientAddToLog() {
	// In a realistic scenario, the client will find the leader node and
	//communicate with it. In this implementation, we are pretending that the client reached
	//out to the server somehow
	// But any new log entries will not be created unless the server /node is a leader
	// isLeader here is a boolean to indicate whether the node is a leader or not
	for {
		fmt.Println("node status is ", node.state, "it is the leader: ", node.state == "leader")
		if node.state == "leader" {
			// lastAppliedIndex here is an int variable that is needed by a node to store the value of the last index it used in the log
			entry := LogEntry{len(node.log), node.currentTerm}
			log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))

			// leader: add entry to log
			node.log = append(node.log, entry)
			// Add rest of logic here
			// HINT 1: using the AppendEntry RPC might happen here
			// leader appends log to servers
			//var prevLogEntry LogEntry
			//if log list is not empty
			// if len(node.log)-1 != 0 {
			// 	prevLogEntry = node.log[len(node.log)-1]
			// 	fmt.Println("Passing")

			// } else { // if log list is empty
			// 	fmt.Println("PASSING EMPTY LOG 2")
			// 	prevLogEntry = LogEntry{0, 0}
			// }
			//fmt.Println("Previous Log Entry: ", prevLogEntry)

			for _, peer := range node.serverNodes {

				followerPrevLogIndex := node.nextIndex[peer.serverID] - 1
				//fmt.Println("I reach here")
				//if peer.serverID != node.selfID {
				if len(node.log)-1 >= node.nextIndex[peer.serverID] { //follower is not up to date
					// Construct arguments for AppendEntry RPC call
					args := AppendEntryArgument{
						Term:         node.currentTerm,
						LeaderID:     node.selfID,
						Entries:      node.log[followerPrevLogIndex+1],
						PrevLogEntry: node.log[followerPrevLogIndex], //prevLogEntry, //fix this so that second argument makes sense
						LeaderCommit: node.commitIndex,
						PrevLogIndex: followerPrevLogIndex,
					}

					// Create a reply variable to store the response
					var reply AppendEntryReply

					// Call AppendEntry RPC on the peer
					err := peer.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
					if err != nil {
						fmt.Printf("Error calling AppendEntry in Client to node %d: %v\n", peer.serverID, err)
						// Handle the error appropriately, e.g., mark the peer as unreachable
					} else {
						if !reply.Success { // keeps running until reply.success = True
							fmt.Println("log entry appending failed for node ", peer.serverID, "try again")

							//if the rpc failed, two cases: either this is not the leader node, or follower node's log is inconsistent

							//if node.currentTerm < reply.Term{
							//issue is that this node is an old leader, msut step down
							//}else{

							//the follower node's log is inconsistent, decrement nextIndex
							node.nextIndex[peer.serverID] -= 1
						} else {
							//append worked, increment nextIndex
							fmt.Print("append entry returned success")
							node.nextIndex[peer.serverID] += 1
						}
					}
				}
			}
			//add a wait for nodes to all finish
			//if majority ovtes yes, check in same way as leader election
			//node.commitIndex += 1

		} else {
			// If the node is no longer the leader, stop sending heartbeats
			fmt.Println("node is not the leader, don't call clientCall")
			// node.Mutex.Unlock()
			//return
		}
		time.Sleep(1000 * time.Millisecond) //40
	}
	// HINT 2: force the thread to sleep for a good amount of time (less
	//than that of the leader election timer) and then repeat the actions above.
	//You may use an endless loop here or recursively call the function
	// HINT 3: you don’t need to add to the logic of creating new log
	//entries, just handle the replication
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (node *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {

	// check the term that is coming in

	node.Mutex.Lock()
	defer node.Mutex.Unlock()
	//fmt.Println("heartbeat from", arguments.LeaderID)
	fmt.Println("arguments to AppendEntry for node ", node.selfID, " : ", arguments)
	// Check if the leader's term is less than receiving
	if arguments.Term < node.currentTerm {
		// Reply false if leader's term is older
		reply.Term = node.currentTerm
		reply.Success = false

	} else {
		// Update term to match the leader's term and transition to follower state
		node.currentTerm = arguments.Term
		node.transitionToFollower()

		// Reset the election timeout as the leader is now active
		node.resetElectionTimeout()

		fmt.Println("in term ", arguments.Term, "passed entry: ", arguments.Entries)

		// Reply to the leader with success
		reply.Term = node.currentTerm

		//base case, list is empty
		if len(node.log) == 0 {
			//empty log, append entries
			// is it a heartbeat or what (if entry is)
			fmt.Println("Log is empty")
			node.log = append(node.log, arguments.Entries)
			node.commitIndex += 1
			reply.Success = true

		} else if (len(node.log) - 1) < arguments.PrevLogIndex {
			//this is if the log doesn't contain an entry at prevLogIndex
			reply.Success = false
			fmt.Print("the follower log does not contain an entry at prevLogIndex ", arguments.PrevLogIndex)
		} else if (len(node.log) - 1) > arguments.PrevLogIndex {
			//the follower's log has too many entries
			fmt.Println("LOG IS TOO LONG FOR NODE ", node.selfID)
			//delete any logs that are at a higher index that prevLogIndex,
			//node.log = node.log[0:node.log[prevLogIndex -1]] //delete that entry and all that follow
			if node.log[arguments.PrevLogIndex].Term != arguments.PrevLogTerm {
				node.log = node.log[0 : arguments.PrevLogIndex-1] //delete that entry and all that follow
				reply.Success = false
			}
		} else if (len(node.log) - 1) == arguments.PrevLogIndex { //length of node log is at least as up to date as leader's
			fmt.Println("LOG IS CORRECT LENGTH FOR NODE ", node.selfID)
			if node.log[arguments.PrevLogIndex].Term != arguments.PrevLogTerm {
				fmt.Println("LOG HAS WRONG TERM ENTRY FOR NODE ", node.selfID)
				node.log = node.log[0 : arguments.PrevLogIndex-1] //#delete that entry and all that follow
				reply.Success = false
			} else {
				//append new entries in log
				fmt.Println("APPENDING SUCCEEDS FOR NODE ", node.selfID)
				node.log = append(node.log, arguments.Entries)
				node.commitIndex += 1
				reply.Success = true
			}
		}

		fmt.Println("NODE ", node.selfID, "'S LOG: ", node.log)

		/*
			if ((len(node.log) -1) < prevLogIndex)      0, 1, 2, 3, 4, 5, 6  index = 6 - 1
				#log doesn't contain an entry at prevLogIndex,																						 0, 1, 2, 3, 4
			{
				reply false #client side decrements stuff and retries
			}
			if (len(node.log) -1 > prevLogIndex){
				//delete any logs that are at a higher index that prevLogIndex,
				node.log = node.log[0:node.log[prevLogIndex -1] #delete that entry and all that follow
				if (node.log[prevLogIndex].term != prevLogTerm){
					node.log = node.log[0:node.log[prevLogIndex -1] #delete that entry and all that follow
					return false #so that the client will decrement nextINdex until it gets to the newly shortened lsit for this node
				}
			}else if (len(node.log) -1 == prevLogIndex) #length of node log is at least as up to date as leader's
			{
				if (node.log[prevLogIndex].term != prevLogTerm){
					node.log = node.log[0:node.log[prevLogIndex -1] #delete that entry and all that follow
					return false
				}else{
					#append new entries in log
					return true
				}
			}
		*/

	}

	return nil
}

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (node *RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	// Locking to prevent race conditions
	node.Mutex.Lock()
	defer node.Mutex.Unlock()
	fmt.Printf("Node %d received vote request from Node %d with term %d\n", node.selfID, arguments.CandidateID, arguments.Term)

	// Check that the term of the requester is higher
	if arguments.Term > node.currentTerm {
		node.currentTerm = arguments.Term // receiver node will update current term to match canddiate's term
		node.votedFor = -1                // reset this count since candidate's term is larger

		// Acknowledging vote or not to the candidate
		reply.Term = node.currentTerm
		reply.ResultVote = true               // receiver node will vote yes
		node.votedFor = arguments.CandidateID // Vote for the candidate
		fmt.Printf("Node %d voted for Node %d in term %d\n", node.selfID, arguments.CandidateID, arguments.Term)

	} else {
		fmt.Println(node.selfID, " rejected vote")
		reply.Term = node.currentTerm
		reply.ResultVote = false // Vote not granted
	}
	return nil
}

// resetting the election timeout to a random duration
func (node *RaftNode) resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+150) * time.Millisecond
	//if node.electionTimeout != nil {
	//	node.electionTimeout.Stop()
	//}
	node.electionTimeout.Stop()
	node.electionTimeout.Reset(duration)
	// used when transitioning to candidate state
	// node.electionTimeout = time.AfterFunc(duration, func() {
	// 	node.transitionToCandidate()
	// })
}

func (node *RaftNode) transitionToFollower() {
	node.state = "follower"
}

func (node *RaftNode) transitionToCandidate() {
	node.state = "candidate"
	node.votedFor = node.selfID
	// node.resetElectionTimeout()
}

func (node *RaftNode) transitionToLeader() {
	node.state = "leader"
	node.resetElectionTimeout() // Reset the election timeout since the node is now leader
	// Initialize variables

	for _, peer := range node.serverNodes {
		node.nextIndex[int(peer.serverID)] = len(node.log) + 1
		node.matchIndex[peer.serverID] = 0
	}
	go Heartbeat(node, node.serverNodes)

}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func (node *RaftNode) LeaderElection() {
	node.transitionToCandidate()
	// not do anything until timer timesout
	fmt.Printf("Node %d starts leader election\n", node.selfID)
	node.Mutex.Lock()
	node.currentTerm++
	node.votedFor = node.selfID
	node.Mutex.Unlock()

	// node stops receiving heartbeats, notices something is wrong
	arguments := VoteArguments{
		Term:        node.currentTerm,
		CandidateID: node.selfID,
	}
	var reply VoteReply
	// Count of received votes
	votesReceived := 1

	// sending nil as vote, but how do we handle that, what do I pass?
	fmt.Printf("Node %d requests votes for term %d\n", node.selfID, arguments.Term)

	var wg sync.WaitGroup
	// Start vote requests to other nodes
	for _, server := range node.serverNodes {
		wg.Add(1)
		go func(server ServerConnection) {
			defer wg.Done()
			err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
			if err != nil {
				fmt.Printf("Error sending vote request to node %d: %v\n", server.serverID, err)
				return
			}
			if reply.Term > node.currentTerm {
				node.Mutex.Lock()
				node.currentTerm = reply.Term
				node.votedFor = -1
				node.Mutex.Unlock()
				return
			}
			if reply.Term == node.currentTerm && reply.ResultVote {
				node.Mutex.Lock()
				votesReceived++
				node.Mutex.Unlock()
			}
		}(server)
	}
	wg.Wait()

	if votesReceived > len(node.serverNodes)/2 {
		node.Mutex.Lock()
		fmt.Printf("Node %d becomes leader for term %d\n", node.selfID, node.currentTerm)
		node.Mutex.Unlock()
		node.transitionToLeader()

	} else {
		fmt.Printf("Node %d failed to become leader for term %d\n", node.selfID, node.currentTerm)
		node.transitionToFollower()
	}

}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat(node *RaftNode, peers []ServerConnection) {
	for { // infinite loop
		// Lock the node's state for consistency
		node.Mutex.Lock()

		// Check if the node is the leader
		if node.state == "leader" && node.votedFor == node.selfID {
			// Unlock the node's state before sending heartbeats
			node.Mutex.Unlock()

			// If the node is the leader, it'll send a heartbeat message to all other nodes
			for _, peer := range peers {
				//if peer.serverID != node.selfID {
				// Construct arguments for AppendEntry RPC call
				args := AppendEntryArgument{
					Term:         node.currentTerm,
					LeaderID:     node.selfID,
					Entries:      LogEntry{0, 0},
					PrevLogEntry: LogEntry{0, 0},
					LeaderCommit: 0,
				}

				// Create a reply variable to store the response
				var reply AppendEntryReply

				// Call AppendEntry RPC on the peer
				err := peer.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
				if err != nil {
					fmt.Printf("Error sending heartbeat to node %d: %v\n", peer.serverID, err)
					// Handle the error appropriately, e.g., mark the peer as unreachable
				} else {
					//fmt.Printf("Sent heartbeat to node %d\n", peer.Address
				}
				//}
			}
		} else {
			// If the node is no longer the leader, stop sending heartbeats
			node.Mutex.Unlock()
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func main() {

	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// initialize parameters
	node := &RaftNode{
		selfID:      myID,
		currentTerm: 0,
		state:       "follower",
		votedFor:    -1,
		Mutex:       sync.Mutex{},
		commitIndex: 0,
		log:         make([]LogEntry, 0),
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	//api := new(RaftNode)
	err = rpc.Register(node)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			// log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		node.serverNodes = append(node.serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	/*
		**NOTES: Can we save the connections in a map?
		How do we make more than one node initialize the election? Is that something we have to implement?
		For now, let's just assume only one starts the election
	*/

	// call leader election like in peer-pressure, and add the logic in it

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here
	// go node.startElectionTimer()

	//start a timer for every node

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout = time.NewTimer(tRandom)

	go func() {
		<-node.electionTimeout.C

		fmt.Println("start leader election from main timeout")
		node.LeaderElection()
	}()

	go node.ClientAddToLog()
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
	// ** Once leader election ends, then check if 'I am the leader'
	// ** If node is leader, then call heartbeats all the time

}

/*
1. client reaches out to servers
2. if server is leader, it will append that entry to its log
3. it will send that entry via AppendEntry to all other servers
	Case 1: Missing entries
	a. for each server, if server's last term is outdated, it fails, and leader will decrement
	next index and try again until they find a match.
	Case 2: Wrong entries (mismatch)
	a. for each server, if server's last entry is diff but at the same index as leader's, then delete,
	delete entry and all that follow until a match is found
	b. Delete any entries after that point and replace with the
	append any new entries not already in the log, until the server's last log index == leader's commitIndex
	update node's committedIndex every appendEntry to be either leader's commitIndex or index of last new enty
4. If majority of the servers replicate it then leader commits entry, notifies servers it has committed

*/
