package chord

import (
	"math/rand"

	"github.com/sirupsen/logrus"
)

// init a new node with given address
func (node *ChordNode) Init(addr string) {
	node.Addr = addr
	node.data = make(map[string]string)
	node.replica = make(map[string]string)
	node.ID = toID(addr)
	node.fingerTable = [fingerTableSize]string{}
	node.successorList = [successorListSize]string{}
	node.predecessor = ""
	node.nextFinger = 0
	node.shutdown = make(chan struct{})
	node.randGenerator = rand.New(rand.NewSource(int64(node.ID)))
	node.isActive = false
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
func (node *ChordNode) ForceQuit() {
	logrus.Infof("[%s] Force quitting the Chord ring", node.Addr)

	// Do nothing.

	node.StopRPCServer()
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

// gracefully leave the Chord ring
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
func (node *ChordNode) Put(key string, value string) bool {
	keyID := toID(key)
	target := node.findSuccessor(keyID)

	if target == node.Addr {
		node.dataLock.Lock()
		node.data[key] = value
		node.dataLock.Unlock()
		return true
	}

	err := node.RemoteCall(target, "ChordNode.PutData", Pair{Key: key, Value: value}, &struct{}{})
	return err == nil
}

// fetch the value for a key
func (node *ChordNode) Get(key string) (bool, string) {
	keyID := toID(key)
	target := node.findSuccessor(keyID)

	if target == node.Addr {
		// Check primary data first.
		node.dataLock.RLock()
		val, ok := node.data[key]
		node.dataLock.RUnlock()
		if ok {
			return true, val
		}
		node.replicaLock.RLock()
		val, ok = node.replica[key]
		node.replicaLock.RUnlock()
		return ok, val
	}

	var val string
	err := node.RemoteCall(target, "ChordNode.GetValue", key, &val)
	if err != nil {
		// The primary node doesn't have the key.  Try the successor, whose
		// replica store should contain a copy (propagated by stabilize).
		var succList [successorListSize]string
		if e := node.RemoteCall(target, "ChordNode.GetSuccessorList", "", &succList); e == nil {
			succ := succList[0]
			if succ != "" && succ != node.Addr && succ != target {
				if e2 := node.RemoteCall(succ, "ChordNode.GetReplica", key, &val); e2 == nil {
					return true, val
				}
			}
		}
		return false, ""
	}
	return true, val
}

// remove a key-value pair
func (node *ChordNode) Delete(key string) bool {
	keyID := toID(key)
	target := node.findSuccessor(keyID)

	if target == node.Addr {
		// Delete from local primary data.
		node.dataLock.Lock()
		_, ok := node.data[key]
		if ok {
			delete(node.data, key)
		}
		node.dataLock.Unlock()

		if ok {
			node.replicaLock.Lock()
			delete(node.replica, key)
			node.replicaLock.Unlock()
		}

		// Cascade-delete replicas starting from our successor.
		if ok {
			node.mu.Lock()
			succ := node.successorList[0]
			node.mu.Unlock()
			if succ != "" && succ != node.Addr {
				// Retry once on transient RPC failures so that the
				// replica on the successor is reliably removed.
				if err := node.RemoteCall(succ, "ChordNode.DeleteReplica", key, &struct{}{}); err != nil {
					node.RemoteCall(succ, "ChordNode.DeleteReplica", key, &struct{}{})
				}
			}
		}
		return ok
	}

	// Delete from the remote primary node.
	err := node.RemoteCall(target, "ChordNode.DeleteData", key, &struct{}{})
	if err != nil {
		err2 := node.RemoteCall(target, "ChordNode.DeleteReplica", key, &struct{}{})
		if err2 != nil {
			return false
		}
	}

	// Cascade-delete replicas starting from the primary's successor.
	var succList [successorListSize]string
	if e := node.RemoteCall(target, "ChordNode.GetSuccessorList", "", &succList); e == nil {
		succ := succList[0]
		if succ != "" && succ != node.Addr && succ != target {
			if err := node.RemoteCall(succ, "ChordNode.DeleteReplica", key, &struct{}{}); err != nil {
				node.RemoteCall(succ, "ChordNode.DeleteReplica", key, &struct{}{})
			}
		}
	}

	node.replicaLock.Lock()
	delete(node.replica, key)
	node.replicaLock.Unlock()

	return true
}
