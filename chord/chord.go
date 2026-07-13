package chord

import (
	"hash/fnv"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
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

	listener    net.Listener
	server      *rpc.Server
	data        map[string]string
	dataLock    sync.RWMutex
	replica     map[string]string // replica data for fault tolerance
	replicaLock sync.RWMutex

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

type Range struct {
	Start uint32 // exclusive lower bound
	End   uint32 // inclusive upper bound
}

// toID converts a string key to a uint32 ID using the FNV-1a hash function.
func toID(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// helper function to check if an ID is in the range of (ID1, ID2] in the Chord ring
func inRange(id, start, end uint32) bool {
	if start < end {
		return id > start && id <= end
	}
	return id > start || id <= end
}
