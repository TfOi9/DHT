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

// RunRPCServer starts the RPC server for the Chord node
func (node *ChordNode) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

// StopRPCServer stops the RPC server for the Chord node
func (node *ChordNode) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

// Run starts the main loop of the Chord node, handling stabilization, finger fixing, and predecessor checking
func (node *ChordNode) Run(wg *sync.WaitGroup) {
	// listen to RPC requests

	// start loop tasks: stabilization, fix fingers, check predecessor

	// accept loop

	// TBD
}
