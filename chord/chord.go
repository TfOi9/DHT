package chord

import (
	"hash/fnv"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	fingerTableSize       = 32                     // Size of the finger table (2^m, where m is the number of bits in the ID space)
	successorListSize     = 12                     // Size of the successor list for fault tolerance
	rpcTimeout            = 10 * time.Second       // Timeout for RPC calls
	stabilizationInterval = 500 * time.Millisecond // Interval for stabilization routine
	fixFingerInterval     = 1 * time.Second        // Interval for fixing finger table entries
	checkPredInterval     = 1 * time.Second        // Interval for checking predecessor liveness
)

type ChordNode struct {
	Addr   string // address and port number of the node, e.g., "localhost:1234"
	online bool

	listener net.Listener
	server   *rpc.Server
	data     map[string]string
	dataLock sync.RWMutex

	ID            uint32                    // Unique identifier for the node in the Chord ring
	fingerTable   [fingerTableSize]string   // Finger table for efficient lookups
	successorList [successorListSize]string // List of successors for fault tolerance
	predecessor   string                    // Predecessor node ID
	nextFinger    int                       // Index of the next finger to fix

	mu sync.Mutex // Mutex for synchronizing access to the node's state

	shutdown chan struct{} // Channel to signal shutdown

	randGenerator *rand.Rand // Random number generator for node operations
	isActive      bool       // Flag to indicate if the node is active in the Chord ring
}

type Pair struct {
	Key   string
	Value string
}

// toID converts a string key to a uint32 ID using the FNV-1a hash function.
func toID(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// init a new node with given address
func (node *ChordNode) Init(addr string) {
	node.Addr = addr
	node.data = make(map[string]string)
	node.ID = toID(addr)
	node.fingerTable = [fingerTableSize]string{}
	node.successorList = [successorListSize]string{}
	node.predecessor = ""
	node.nextFinger = 0
	node.shutdown = make(chan struct{})
	node.randGenerator = rand.New(rand.NewSource(int64(node.ID)))
	node.isActive = false
}

// fetch a node's ID safely
func (node *ChordNode) GetID(_ string, reply *uint32) error {
	*reply = node.ID
	return nil
}

// fetch a node's predecessor safely
func (node *ChordNode) GetPredecessor(_ string, reply *string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	*reply = node.predecessor
	return nil
}

// fetch a node's successor list safely
func (node *ChordNode) GetSuccessorList(_ string, reply *[successorListSize]string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	*reply = node.successorList
	return nil
}

// helper function to check if an ID is in the range of (ID1, ID2] in the Chord ring
func inRange(id, start, end uint32) bool {
	if start < end {
		return id > start && id <= end
	}
	return id > start || id <= end
}

// fetch a node's data safely
func (node *ChordNode) GetData(_ string, reply *map[string]string) error {
	node.dataLock.RLock()
	*reply = node.data
	node.dataLock.RUnlock()
	return nil
}

// fetch a node's finger table safely
func (node *ChordNode) GetFingerTable(_ string, reply *[fingerTableSize]string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	*reply = node.fingerTable
	return nil
}

// empty function to check if a node is online
func (node *ChordNode) Ping(_ string, _ *struct{}) error {
	return nil
}

// helper function to perform a remote RPC call to another node
func (node *ChordNode) RemoteCall(addr, method string, args interface{}, reply interface{}) error {
	if method != "ChordNode.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	conn, err := net.DialTimeout("tcp", addr, rpcTimeout)
	if err != nil {
		logrus.Error("dialing: ", err)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}
	return nil
}

// StopRPCServer stops the RPC server for the Chord node
func (node *ChordNode) StopRPCServer() {
	node.online = false
	node.listener.Close()
	close(node.shutdown)
}

// wrapper function for the stabilization loop
func (node *ChordNode) stabilizeLoop() {
	ticker := time.NewTicker(stabilizationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			node.stabilize()
		case <-node.shutdown:
			return
		}
	}
}

// wrapper function for the fix finger loop
func (node *ChordNode) fixFingerLoop() {
	ticker := time.NewTicker(fixFingerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			node.fixFingers()
		case <-node.shutdown:
			return
		}
	}
}

// wrapper function for the check predecessor loop
func (node *ChordNode) checkPredecessorLoop() {
	ticker := time.NewTicker(checkPredInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			node.checkPredecessor()
		case <-node.shutdown:
			return
		}
	}
}

// Run starts the main loop of the Chord node, handling stabilization, finger fixing, and predecessor checking
func (node *ChordNode) Run(wg *sync.WaitGroup) {
	node.online = true

	// listen to RPC requests
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}

	// start loop tasks: stabilization, fix fingers, check predecessor
	go node.stabilizeLoop()
	go node.fixFingerLoop()
	go node.checkPredecessorLoop()

	// accept loop
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

// stabilize checks the node's successor and updates its predecessor and successor list accordingly
func (node *ChordNode) stabilize() {
	// TBD
}

// fixFingers updates the node's finger table entries to ensure efficient lookups
func (node *ChordNode) fixFingers() {
	// TBD
}

// checkPredecessor checks if the node's predecessor is still alive and updates it if necessary
func (node *ChordNode) checkPredecessor() {
	// TBD
}

// initialize a new Chord ring with the current node as the only member
func (node *ChordNode) Create() {
	node.mu.Lock()
	node.successorList[0] = node.Addr
	node.predecessor = ""
	node.fingerTable[0] = node.Addr
	node.isActive = true
	node.mu.Unlock()
	logrus.Infof("[%s] Created a new Chord ring", node.Addr)
}

// force the current node to leave the ring
func (node *ChordNode) ForceQuit() {
	logrus.Infof("[%s] Force quitting the Chord ring", node.Addr)
	node.StopRPCServer()
}

// find the successor of the given ID in the ring
func (node *ChordNode) FindSuccessor(id uint32, reply *string) error {
	node.mu.Lock()
	succ := node.successorList[0]
	node.mu.Unlock()

	// Not part of any ring yet — return self as the only available node.
	if succ == "" {
		*reply = node.Addr
		return nil
	}

	// Case 1: id falls in (self, successor] — successor is the answer.
	if inRange(id, node.ID, toID(succ)) {
		*reply = succ
		return nil
	}

	// Case 2: forward the query to the closest preceding node.
	closest := node.closestPrecedingNode(id)
	if closest == "" || closest == node.Addr {
		// No closer node found — self is the best answer we can give.
		*reply = node.Addr
		return nil
	}

	var nextReply string
	err := node.RemoteCall(closest, "ChordNode.FindSuccessor", id, &nextReply)
	if err != nil {
		// RPC failed — fall back to self.
		*reply = node.Addr
		return nil
	}

	*reply = nextReply
	return nil
}

// find the closest preceding node for the given ID in the ring
func (node *ChordNode) closestPrecedingNode(id uint32) string {
	node.mu.Lock()
	defer node.mu.Unlock()

	for i := fingerTableSize - 1; i >= 0; i-- {
		entry := node.fingerTable[i]
		if entry != "" && inRange(toID(entry), node.ID, id) {
			return entry
		}
	}

	for i := 0; i < successorListSize; i++ {
		entry := node.successorList[i]
		if entry != "" && inRange(toID(entry), node.ID, id) {
			return entry
		}
	}

	return ""
}

func (node *ChordNode) findSuccessor(id uint32) string {
	var reply string
	err := node.FindSuccessor(id, &reply)
	if err != nil {
		return node.Addr
	}
	return reply
}

func (node *ChordNode) Join(addr string) bool {
	logrus.Infof("Join %s", addr)

	node.mu.Lock()
	if node.isActive {
		node.mu.Unlock()
		return false
	}
	node.mu.Unlock()

	var succAddr string
	err := node.RemoteCall(addr, "ChordNode.FindSuccessor", node.ID, &succAddr)
	if err != nil {
		logrus.Errorf("[%s] Failed to find successor from %s: %v", node.Addr, addr, err)
		return false
	}

	node.mu.Lock()
	node.successorList[0] = succAddr

	var remoteList [successorListSize]string
	err = node.RemoteCall(succAddr, "ChordNode.GetSuccessorList", "", &remoteList)
	if err == nil {
		// Merge remote successor list into local list, skipping duplicates and self.
		idx := 1
		for i := 0; i < successorListSize && idx < successorListSize; i++ {
			if remoteList[i] != "" && remoteList[i] != succAddr && remoteList[i] != node.Addr {
				node.successorList[idx] = remoteList[i]
				idx++
			}
		}
		// Clear remaining slots
		for i := idx; i < successorListSize; i++ {
			node.successorList[i] = ""
		}
	}

	node.predecessor = ""
	node.isActive = true
	node.mu.Unlock()

	logrus.Infof("[%s] Joined the Chord ring via %s, successor: %s", node.Addr, addr, succAddr)
	return true
}
