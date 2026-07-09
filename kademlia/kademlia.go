package kademlia

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

// constants
const (
	IDLength          = 20               // SHA1 has length of 160 bits, which is 20 bytes
	KBucketCount      = 160              // Every k-bucket corresponds to a bit in the node ID
	K                 = 20               // Number of nodes to store in each k-bucket
	Alpha             = 3                // Number of concurrent requests allowed
	RPCTimeout        = 10 * time.Second // Timeout for RPC calls
	RefreshInterval   = 1 * time.Hour    // Interval for refreshing k-buckets
	RepublishInterval = 24 * time.Hour   // Interval for republishing data
)

// Kademlia node
type KademliaNode struct {
	Addr     string
	online   bool
	listener net.Listener
	server   *rpc.Server

	data     map[string]string
	dataLock sync.RWMutex

	NodeID      [IDLength]byte         // SHA-1 hash of the node's address
	buckets     [KBucketCount]*KBucket // k-buckets
	bucketsLock sync.RWMutex

	isActive bool

	mu sync.Mutex

	shutdown chan struct{}

	republishMap     map[string]time.Time // Map to track republished data and their timestamps
	republishMapLock sync.Mutex
}
