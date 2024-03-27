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
	Term     int
	LeaderID int
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

type RaftNode struct {
	selfID          int
	serverNodes     []ServerConnection
	currentTerm     int
	votedFor        int
	state           string // "follower", "candidate", "leader"
	Mutex           sync.Mutex
	electionTimeout *time.Timer
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (node *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	node.Mutex.Lock()
	defer node.Mutex.Unlock()
	fmt.Println("heartbeat from", arguments.LeaderID)
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

		// Next assignment implmentation

		// Reply to the leader with success
		reply.Term = node.currentTerm
		reply.Success = true
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
	if node.electionTimeout != nil {
		node.electionTimeout.Stop()
	}
	// used when transitioning to candidate state
	node.electionTimeout = time.AfterFunc(duration, func() {
		node.transitionToCandidate()
	})
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

	// Uncomment this to simulate leader failure
	// timer := time.NewTimer(5 * time.Second)

	// // Wait for the timer to expire
	// <-timer.C

	// // Timer has expired
	// fmt.Println("Simulating leader failure")
	// node.transitionToFollower()

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
				if peer.serverID != node.selfID {
					// Construct arguments for AppendEntry RPC call
					args := AppendEntryArgument{
						Term:     node.currentTerm,
						LeaderID: node.selfID,
					}

					// Create a reply variable to store the response
					var reply AppendEntryReply

					// Call AppendEntry RPC on the peer
					err := peer.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
					if err != nil {
						fmt.Printf("Error sending heartbeat to node %d: %v\n", peer.serverID, err)
						// Handle the error appropriately, e.g., mark the peer as unreachable
					} else {
						fmt.Printf("Sent heartbeat to node %d\n", peer.serverID)
					}
				}
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

	timerExpired := make(chan bool)

	timer := func(timerExpired chan<- bool) {
		// Generate a random duration for the timer
		tRandom := time.Duration(rand.Intn(150)+151) * time.Millisecond
		fmt.Println("Timer set for", tRandom)

		// Wait for the random duration
		<-time.After(tRandom)

		// Signal timer expiration
		timerExpired <- true
	}

	// Start the timer in a Goroutine
	go timer(timerExpired)

	// Wait for the timer to expire
	<-timerExpired

	// Call leader election function when the timer expires
	var wg sync.WaitGroup
	wg.Add(1)
	go node.LeaderElection()
	wg.Wait()
	// ** Once leader election ends, then check if 'I am the leader'
	// ** If node is leader, then call heartbeats all the time

}
