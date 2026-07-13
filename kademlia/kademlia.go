package kademlia

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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

// Initializes a new Kademlia node with the given address
func (node *KademliaNode) Init(addr string) {
	node.Addr = addr
	node.NodeID = hash(addr)
	node.data = make(map[string]string)
	node.shutdown = make(chan struct{})
	node.republishMap = make(map[string]time.Time)

	for i := 0; i < KBucketCount; i++ {
		node.buckets[i] = &KBucket{
			contacts: []Contact{},
		}
	}
}

// Create a new Kademlia network
func (node *KademliaNode) Create() {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.isActive = true
	logrus.Infof("[%s] Created a new Kademlia network", node.Addr)
}

// Run a Kademlia node
func (node *KademliaNode) Run(wg *sync.WaitGroup) {
	node.online = true

	node.NodeID = hash(node.Addr)
	node.data = make(map[string]string)
	node.shutdown = make(chan struct{})
	node.republishMap = make(map[string]time.Time)

	node.server = rpc.NewServer()
	node.server.Register(node)

	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	if err != nil {
		logrus.Errorf("[%s] Failed to listen on %s: %v", node.Addr, node.Addr, err)
		wg.Done()
		return
	}
	wg.Done()

	go node.bucketRefreshLoop()

	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Errorf("[%s] Failed to accept connection: %v", node.Addr, err)
			continue
		}
		go node.server.ServeConn(conn)
	}
}

// Let a new node join the Kademlia network by contacting an existing node
func (node *KademliaNode) Join(existingNodeAddr string) bool {
	logrus.Infof("[%s] Joining the Kademlia network via %s", node.Addr, existingNodeAddr)

	node.mu.Lock()
	if node.isActive {
		node.mu.Unlock()
		logrus.Warnf("[%s] Node is already active. Join operation aborted.", node.Addr)
		return false
	}
	node.mu.Unlock()

	err := node.RemoteCall(existingNodeAddr, "KademliaNode.Ping", "", &struct{}{})
	if err != nil {
		logrus.Errorf("[%s] Bootstrap node %s is unreachable: %v", node.Addr, existingNodeAddr, err)
		return false
	}

	node.updateRoutingTable(existingNodeAddr)

	contacts := node.findNode(node.NodeID)

	if len(contacts) > 0 {
		// pull data from the closest node
		// TBD
	}

	node.mu.Lock()
	node.isActive = true
	node.mu.Unlock()

	logrus.Infof("[%s] Successfully joined the Kademlia network", node.Addr)
	return true
}

// Helper function to pull data from the closest node
func (node *KademliaNode) pullDataFromClosest(closestNodeAddr string) {
	var allData map[string]string
	err := node.RemoteCall(closestNodeAddr, "KademliaNode.GetAllData", "", &allData)
	if err != nil {
		logrus.Errorf("[%s] Failed to pull data from %s: %v", node.Addr, closestNodeAddr, err)
		return
	}

	for key, value := range allData {
		keyHash := hash(key)
		myDistance := xorDistance(node.NodeID, keyHash)
		closestDistance := xorDistance(hash(closestNodeAddr), keyHash)

		if myDistance.Cmp(closestDistance) < 0 {
			node.dataLock.Lock()
			node.data[key] = value
			node.dataLock.Unlock()
		}
	}

	logrus.Infof("[%s] Successfully pulled data from %s", node.Addr, closestNodeAddr)
}

// Gracefully shut down the Kademlia node
func (node *KademliaNode) Quit() {
	node.mu.Lock()
	if !node.isActive {
		node.mu.Unlock()
		logrus.Warnf("[%s] Node is not active. Quit operation aborted.", node.Addr)
		return
	}
	node.isActive = false
	node.mu.Unlock()

	node.dataLock.RLock()
	keys := make([]string, 0, len(node.data))
	values := make([]string, 0, len(node.data))
	for key, value := range node.data {
		keys = append(keys, key)
		values = append(values, value)
	}
	node.dataLock.RUnlock()

	// Redistribute each key to Alpha closest LIVE nodes (from local routing table).
	// Try up to K contacts but stop after Alpha successful stores to balance
	// durability and performance.
	for i := 0; i < len(keys); i++ {
		keyID := hash(keys[i])
		closest := node.findClosestContacts(keyID, K)
		successCnt := 0
		for _, c := range closest {
			if c.Addr == node.Addr {
				continue
			}
			err := node.RemoteCall(c.Addr, "KademliaNode.Store", &StoreArgs{Key: keys[i], Value: values[i]}, &struct{}{})
			if err != nil {
				logrus.Errorf("[%s] Failed to store data on %s: %v", node.Addr, c.Addr, err)
			} else {
				logrus.Infof("[%s] Successfully stored data on %s", node.Addr, c.Addr)
				successCnt++
				if successCnt >= Alpha {
					break
				}
			}
		}
	}

	node.stopRPCServer()
}

// Forces a node to quit
func (node *KademliaNode) ForceQuit() {
	// Do nothing.

	node.stopRPCServer()
}
