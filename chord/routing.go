package chord

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

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
		for {
			if node.successorList[0] == "" || node.successorList[0] == node.Addr {
				break
			}
			node.mu.Unlock()
			err2 := node.RemoteCall(node.successorList[0], "ChordNode.Ping", "", &struct{}{})
			node.mu.Lock()
			if err2 == nil {
				break
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
		return
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

	// Copy own data (the primary source of replicas).
	node.dataLock.RLock()
	dataCopy := make(map[string]string)
	for k, v := range node.data {
		dataCopy[k] = v
	}
	node.dataLock.RUnlock()

	// Send primary data to the successor as replicas.
	for k, v := range dataCopy {
		var dummy struct{}
		node.RemoteCall(succ, "ChordNode.PutReplica", Pair{Key: k, Value: v}, &dummy)
	}
}

// fixFingers updates the node's finger table entries to ensure efficient lookups.
func (node *ChordNode) fixFingers() {
	// Randomly pick a finger entry to refresh
	node.mu.Lock()
	i := node.randGenerator.Intn(fingerTableSize)
	node.mu.Unlock()

	fingerID := node.ID + (1 << i)
	addr := node.findSuccessor(fingerID)

	// If findSuccessor returned self and we have no successors, the node is
	// isolated — keep the old finger entry (which may still point to a live
	// node) instead of blindly overwriting it with self.
	node.mu.Lock()
	if addr == node.Addr && node.successorList[0] == "" {
		node.mu.Unlock()
	} else {
		node.fingerTable[i] = addr
		node.mu.Unlock()
	}

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

// checkPredecessor checks if the node's predecessor is still alive.
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
		if node.predecessor == pred {
			node.predecessor = ""
			logrus.Warnf("[%s] Predecessor %s is dead, cleared", node.Addr, pred)
		}
		node.mu.Unlock()
	}
}

// find the successor of the given ID in the ring
func (node *ChordNode) FindSuccessor(id uint32, reply *string) error {
	node.mu.Lock()
	succ := node.successorList[0]
	node.mu.Unlock()

	// Not part of any ring yet — return self as the only available node.
	if succ == "" {
		// The successor list is empty (all known successors are dead).
		// Before giving up, try the finger table — it may still contain
		// entries pointing to live nodes that can help route the query.
		node.mu.Lock()
		fingers := node.fingerTable
		node.mu.Unlock()
		for _, f := range fingers {
			if f == "" || f == node.Addr {
				continue
			}
			var nextReply string
			if err := node.RemoteCall(f, "ChordNode.FindSuccessor", id, &nextReply); err == nil {
				*reply = nextReply
				return nil
			}
		}
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

// UpdatePredecessor sets the predecessor to newPred.
func (node *ChordNode) UpdatePredecessor(newPred string, _ *struct{}) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if newPred == "" || newPred == node.Addr {
		return nil
	}
	node.predecessor = newPred
	return nil
}

// UpdateSuccessor inserts newSucc as the direct successor, shifting the
// existing successorList entries right. Duplicates and self are excluded.
func (node *ChordNode) UpdateSuccessor(newSucc string, _ *struct{}) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if newSucc == "" || newSucc == node.Addr {
		return nil
	}

	for i := 0; i < successorListSize; i++ {
		if node.successorList[i] == newSucc {
			for j := i; j < successorListSize-1; j++ {
				node.successorList[j] = node.successorList[j+1]
			}
			node.successorList[successorListSize-1] = ""
			break
		}
	}

	for i := successorListSize - 1; i > 0; i-- {
		node.successorList[i] = node.successorList[i-1]
	}
	node.successorList[0] = newSucc
	return nil
}
