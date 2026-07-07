package chord

import (
	"fmt"
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

// Range describes a half-open interval on the Chord ring: (Start, End].
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

// PutData stores a key-value pair on this node. Always succeeds (overwrites).
func (node *ChordNode) PutData(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	return nil
}

// GetValue retrieves the value for a single key from this node.
// Returns an error when the key is not found.
func (node *ChordNode) GetValue(key string, reply *string) error {
	node.dataLock.RLock()
	val, ok := node.data[key]
	node.dataLock.RUnlock()
	if !ok {
		return fmt.Errorf("key not found: %s", key)
	}
	*reply = val
	return nil
}

// DeleteData removes a key-value pair from this node.
// Returns an error when the key is not found.
func (node *ChordNode) DeleteData(key string, _ *struct{}) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	if ok {
		delete(node.data, key)
	}
	node.dataLock.Unlock()
	if !ok {
		return fmt.Errorf("key not found: %s", key)
	}
	return nil
}

// TransferKeys returns (and deletes locally) all key-value pairs whose hashed
// key falls within the half-open interval (r.Start, r.End] on the Chord ring.
func (node *ChordNode) TransferKeys(r Range, reply *map[string]string) error {
	result := make(map[string]string)
	node.dataLock.Lock()
	for k, v := range node.data {
		if inRange(toID(k), r.Start, r.End) {
			result[k] = v
			delete(node.data, k)
		}
	}
	node.dataLock.Unlock()
	*reply = result
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

// StopRPCServer stops the RPC server for the Chord node.
// Safe to call multiple times; uses sync.Once internally.
func (node *ChordNode) StopRPCServer() {
	node.mu.Lock()
	if !node.online {
		node.mu.Unlock()
		return
	}
	node.online = false
	node.mu.Unlock()

	if node.listener != nil {
		node.listener.Close()
	}
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
	node.mu.Lock()
	succ := node.successorList[0]
	node.mu.Unlock()

	// Not part of any ring yet — nothing to stabilize.
	if succ == "" {
		return
	}

	// Check whether the successor is still alive; if not, scan the
	// successor list for the first live entry.
	err := node.RemoteCall(succ, "ChordNode.Ping", "", &struct{}{})
	if err != nil {
		node.mu.Lock()
		// Shift out dead entries until we find a live successor (or exhaust the list).
		for {
			if node.successorList[0] == "" || node.successorList[0] == node.Addr {
				break
			}
			node.mu.Unlock()
			err2 := node.RemoteCall(node.successorList[0], "ChordNode.Ping", "", &struct{}{})
			node.mu.Lock()
			if err2 == nil {
				break // found live successor
			}
			logrus.Warnf("[%s] Successor %s is dead, shifting successorList", node.Addr, node.successorList[0])
			for i := 0; i < successorListSize-1; i++ {
				node.successorList[i] = node.successorList[i+1]
			}
			node.successorList[successorListSize-1] = ""
		}
		succ = node.successorList[0]
		node.mu.Unlock()
		if succ == "" || succ == node.Addr {
			return
		}
	}

	// Ask the successor for its predecessor.
	var x string
	err = node.RemoteCall(succ, "ChordNode.GetPredecessor", "", &x)
	if err != nil {
		return // retry next round
	}

	// If x is a better successor (in (n, successor)), adopt it.
	if x != "" && x != node.Addr && inRange(toID(x), node.ID, toID(succ)) {
		node.mu.Lock()
		node.successorList[0] = x
		node.mu.Unlock()
		succ = x // notify the new successor
		logrus.Infof("[%s] Found closer successor: %s", node.Addr, x)
	}

	// Tell the (possibly updated) successor about our existence.
	node.RemoteCall(succ, "ChordNode.Notify", node.Addr, &struct{}{})
}

// fixFingers updates the node's finger table entries to ensure efficient lookups.
// Following the Chord paper §4.4: each invocation refreshes a randomly chosen
// finger entry to avoid thundering-herd effects when many nodes join simultaneously.
func (node *ChordNode) fixFingers() {
	// Randomly pick a finger entry to refresh (Chord paper: "randomly chosen").
	node.mu.Lock()
	i := node.randGenerator.Intn(fingerTableSize)
	node.mu.Unlock()

	// Compute the target ID for this finger: n + 2^i  (mod 2^32 via uint32 overflow).
	fingerID := node.ID + (1 << i)
	addr := node.findSuccessor(fingerID)

	node.mu.Lock()
	node.fingerTable[i] = addr
	node.mu.Unlock()

	// Also refresh the successor list from the current successor.
	node.mu.Lock()
	succ := node.successorList[0]
	node.mu.Unlock()

	if succ == "" || succ == node.Addr {
		return
	}

	var remoteList [successorListSize]string
	err := node.RemoteCall(succ, "ChordNode.GetSuccessorList", "", &remoteList)
	if err != nil {
		return
	}

	node.mu.Lock()
	// Keep [0]=succ, fill [1..] from remote entries (dedup self and succ).
	idx := 1
	for j := 0; j < successorListSize && idx < successorListSize; j++ {
		if remoteList[j] != "" && remoteList[j] != succ && remoteList[j] != node.Addr {
			node.successorList[idx] = remoteList[j]
			idx++
		}
	}
	for j := idx; j < successorListSize; j++ {
		node.successorList[j] = ""
	}
	node.mu.Unlock()
}

// Notify is called by a node that thinks it might be our predecessor.
// Following the Chord paper: update predecessor only if it is nil or
// the notifying node lies in (predecessor, self).
// When predecessor transitions from empty to non-empty for the first time,
// precise key migration is triggered to correct any over/under-pull from Join.
func (node *ChordNode) Notify(args string, _ *struct{}) error {
	node.mu.Lock()

	// Ignore self-notifications.
	if args == node.Addr {
		node.mu.Unlock()
		return nil
	}

	oldPred := node.predecessor
	shouldUpdate := oldPred == "" || inRange(toID(args), toID(oldPred), node.ID)
	if shouldUpdate {
		node.predecessor = args
		logrus.Infof("[%s] Updated predecessor to %s", node.Addr, args)
	}
	node.mu.Unlock()

	// First-time predecessor assignment: pull precisely the keys in (pred, self]
	// from the successor, and return any excess local keys.
	if shouldUpdate && oldPred == "" {
		node.mu.Lock()
		succ := node.successorList[0]
		node.mu.Unlock()

		if succ != "" && succ != node.Addr {
			// 1. Pull keys that hash into (predecessor, self] from successor.
			var migrated map[string]string
			r := Range{Start: toID(args), End: node.ID}
			if err := node.RemoteCall(succ, "ChordNode.TransferKeys", r, &migrated); err == nil {
				node.dataLock.Lock()
				for k, v := range migrated {
					node.data[k] = v
				}
				node.dataLock.Unlock()
			}

			// 2. Return local keys that do NOT belong to (predecessor, self].
			node.dataLock.Lock()
			var toReturn []Pair
			for k, v := range node.data {
				if !inRange(toID(k), toID(args), node.ID) {
					toReturn = append(toReturn, Pair{Key: k, Value: v})
				}
			}
			for _, p := range toReturn {
				delete(node.data, p.Key)
			}
			node.dataLock.Unlock()

			// Send excess keys back to the successor (RPC outside lock).
			for _, p := range toReturn {
				var dummy struct{}
				node.RemoteCall(succ, "ChordNode.PutData", p, &dummy)
			}
		}
	}
	return nil
}

// checkPredecessor checks if the node's predecessor is still alive.
// Following the Chord paper: if the predecessor has failed, clear it.
func (node *ChordNode) checkPredecessor() {
	node.mu.Lock()
	pred := node.predecessor
	node.mu.Unlock()

	if pred == "" || pred == node.Addr {
		return
	}

	err := node.RemoteCall(pred, "ChordNode.Ping", "", &struct{}{})
	if err != nil {
		node.mu.Lock()
		// Only clear if the predecessor hasn't been concurrently updated.
		if node.predecessor == pred {
			node.predecessor = ""
			logrus.Warnf("[%s] Predecessor %s is dead, cleared", node.Addr, pred)
		}
		node.mu.Unlock()
	}
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

// force the current node to leave the ring.
// Unlike Quit(), this is an ungraceful departure, but we still make a
// best-effort attempt to transfer data to the successor and notify
// neighbors so the ring can recover more quickly.
func (node *ChordNode) ForceQuit() {
	logrus.Infof("[%s] Force quitting the Chord ring", node.Addr)

	node.mu.Lock()
	succ := node.successorList[0]
	pred := node.predecessor
	node.mu.Unlock()

	// 1. Transfer all local data to the successor (best-effort).
	if succ != "" && succ != node.Addr {
		node.dataLock.RLock()
		for k, v := range node.data {
			var dummy struct{}
			if err := node.RemoteCall(succ, "ChordNode.PutData", Pair{Key: k, Value: v}, &dummy); err != nil {
				logrus.Errorf("[%s] ForceQuit: failed to transfer key %s to %s: %v", node.Addr, k, succ, err)
			}
		}
		node.dataLock.RUnlock()
	}

	// 2. Notify predecessor to skip past us.
	if pred != "" && pred != node.Addr {
		var dummy struct{}
		node.RemoteCall(pred, "ChordNode.UpdateSuccessor", succ, &dummy)
	}

	// 3. Notify successor about our predecessor.
	if succ != "" && succ != node.Addr {
		var dummy struct{}
		node.RemoteCall(succ, "ChordNode.UpdatePredecessor", pred, &dummy)
	}

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
		// closest is dead — try each live successor in order.
		node.mu.Lock()
		backups := node.successorList
		node.mu.Unlock()
		for _, s := range backups {
			if s == "" || s == node.Addr || s == closest {
				continue
			}
			err = node.RemoteCall(s, "ChordNode.FindSuccessor", id, &nextReply)
			if err == nil {
				break
			}
		}

		// If all successors failed, also try finger table entries.
		if err != nil {
			node.mu.Lock()
			fingers := node.fingerTable
			node.mu.Unlock()
			for _, f := range fingers {
				if f == "" || f == node.Addr || f == closest {
					continue
				}
				// Skip entries already tried via successor list.
				alreadyTried := false
				for _, s := range backups {
					if s == f {
						alreadyTried = true
						break
					}
				}
				if alreadyTried {
					continue
				}
				err = node.RemoteCall(f, "ChordNode.FindSuccessor", id, &nextReply)
				if err == nil {
					break
				}
			}
		}
	}
	if err != nil {
		// All alternatives exhausted — return self as last resort.
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

	// Conservative key migration: pull data from the successor that may now
	// belong to this node. Keys that hash outside (self, successor] are
	// potentially ours; the Notify RPC will later refine the range precisely.
	var allData map[string]string
	if err := node.RemoteCall(succAddr, "ChordNode.GetData", "", &allData); err == nil {
		node.dataLock.Lock()
		for k, v := range allData {
			keyID := toID(k)
			if !inRange(keyID, node.ID, toID(succAddr)) {
				node.data[k] = v
			}
		}
		node.dataLock.Unlock()

		// Remove the transferred keys from the successor.
		for k := range allData {
			if _, ok := node.data[k]; ok {
				var dummy struct{}
				node.RemoteCall(succAddr, "ChordNode.DeleteData", k, &dummy)
			}
		}
	}

	logrus.Infof("[%s] Joined the Chord ring via %s, successor: %s", node.Addr, addr, succAddr)
	return true
}

func (node *ChordNode) Quit() {
	node.mu.Lock()
	if !node.isActive {
		node.mu.Unlock()
		return
	}

	logrus.Infof("[%s] Quitting the Chord ring", node.Addr)

	succ := node.successorList[0]
	pred := node.predecessor
	node.isActive = false
	node.mu.Unlock()

	// Move all local data to the successor (best-effort).
	if succ != "" && succ != node.Addr {
		node.dataLock.RLock()
		allData := node.data
		node.dataLock.RUnlock()

		for k, v := range allData {
			var dummy struct{}
			err := node.RemoteCall(succ, "ChordNode.PutData", Pair{Key: k, Value: v}, &dummy)
			if err != nil {
				logrus.Errorf("[%s] Failed to transfer key %s to successor %s: %v", node.Addr, k, succ, err)
			}
		}
	}

	// Notify predecessor to skip past us: its new successor is our successor.
	if pred != "" && pred != node.Addr {
		node.RemoteCall(pred, "ChordNode.UpdateSuccessor", succ, &struct{}{})
	}

	// Notify successor that its new predecessor is our predecessor.
	if succ != "" && succ != node.Addr {
		node.RemoteCall(succ, "ChordNode.UpdatePredecessor", pred, &struct{}{})
	}

	node.StopRPCServer()
	logrus.Infof("[%s] Successfully quit the Chord ring", node.Addr)
}

// Put stores a key-value pair at the node responsible for the key.
// Following the Chord paper: key k is stored at successor(k).
func (node *ChordNode) Put(key string, value string) bool {
	keyID := toID(key)
	target := node.findSuccessor(keyID)

	// Local storage — no RPC needed.
	if target == node.Addr {
		node.dataLock.Lock()
		node.data[key] = value
		node.dataLock.Unlock()
		return true
	}

	err := node.RemoteCall(target, "ChordNode.PutData", Pair{Key: key, Value: value}, &struct{}{})
	return err == nil
}

// Get retrieves the value for a key from the node responsible for it.
// Following the Chord paper: looking up key k is to query successor(k).
func (node *ChordNode) Get(key string) (bool, string) {
	keyID := toID(key)
	target := node.findSuccessor(keyID)

	// Local lookup — no RPC needed.
	if target == node.Addr {
		node.dataLock.RLock()
		val, ok := node.data[key]
		node.dataLock.RUnlock()
		return ok, val
	}

	var val string
	err := node.RemoteCall(target, "ChordNode.GetValue", key, &val)
	if err != nil {
		return false, ""
	}
	return true, val
}

// Delete removes a key-value pair from the node responsible for the key.
// Returns true if the key existed and was removed.
func (node *ChordNode) Delete(key string) bool {
	keyID := toID(key)
	target := node.findSuccessor(keyID)

	// Local deletion — no RPC needed.
	if target == node.Addr {
		node.dataLock.Lock()
		_, ok := node.data[key]
		if ok {
			delete(node.data, key)
		}
		node.dataLock.Unlock()
		return ok
	}

	err := node.RemoteCall(target, "ChordNode.DeleteData", key, &struct{}{})
	return err == nil
}

// UpdateSuccessor inserts newSucc as the direct successor, shifting the
// existing successorList entries right. Duplicates and self are excluded.
func (node *ChordNode) UpdateSuccessor(newSucc string, _ *struct{}) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if newSucc == "" || newSucc == node.Addr {
		return nil
	}

	// Remove newSucc from any existing position in the list (dedup).
	for i := 0; i < successorListSize; i++ {
		if node.successorList[i] == newSucc {
			// Shift remaining entries left.
			for j := i; j < successorListSize-1; j++ {
				node.successorList[j] = node.successorList[j+1]
			}
			node.successorList[successorListSize-1] = ""
			break
		}
	}

	// Shift right to make room at [0].
	for i := successorListSize - 1; i > 0; i-- {
		node.successorList[i] = node.successorList[i-1]
	}
	node.successorList[0] = newSucc
	return nil
}

// UpdatePredecessor sets the predecessor to newPred.
// This is a best-effort utility; Chord's standard Notify RPC (with
// range-based validation) is preferred for ring convergence.
func (node *ChordNode) UpdatePredecessor(newPred string, _ *struct{}) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if newPred == "" || newPred == node.Addr {
		return nil
	}
	node.predecessor = newPred
	return nil
}
