package chord

import (
	"fmt"
	"net"
	"net/rpc"

	"github.com/sirupsen/logrus"
)

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

// fetch a node's data safely
func (node *ChordNode) GetData(_ string, reply *map[string]string) error {
	node.dataLock.RLock()
	copyMap := make(map[string]string, len(node.data))
	for k, v := range node.data {
		copyMap[k] = v
	}
	node.dataLock.RUnlock()
	*reply = copyMap
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

// PutReplica stores a key-value pair in the replica storage of this node. Always succeeds (overwrites).
func (node *ChordNode) PutReplica(pair Pair, _ *struct{}) error {
	node.replicaLock.Lock()
	node.replica[pair.Key] = pair.Value
	node.replicaLock.Unlock()
	return nil
}

// GetValue retrieves the value for a single key from this node.
// Checks primary data first, then falls back to replica storage.
// Returns an error when the key is not found in either store.
func (node *ChordNode) GetValue(key string, reply *string) error {
	node.dataLock.RLock()
	val, ok := node.data[key]
	node.dataLock.RUnlock()
	if ok {
		*reply = val
		return nil
	}
	// Fall back to replica storage (e.g. after predecessor crashed and we
	// became responsible for its key range).
	node.replicaLock.RLock()
	val, ok = node.replica[key]
	node.replicaLock.RUnlock()
	if ok {
		*reply = val
		return nil
	}
	return fmt.Errorf("key not found: %s", key)
}

// GetReplica retrieves the value for a single key from the replica storage of this node.
func (node *ChordNode) GetReplica(key string, reply *string) error {
	node.replicaLock.RLock()
	val, ok := node.replica[key]
	node.replicaLock.RUnlock()
	if !ok {
		return fmt.Errorf("replica key not found: %s", key)
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

func (node *ChordNode) DeleteReplica(key string, _ *struct{}) error {
	node.replicaLock.Lock()
	_, ok := node.replica[key]
	if ok {
		delete(node.replica, key)
	}
	node.replicaLock.Unlock()
	if !ok {
		return fmt.Errorf("replica key not found: %s", key)
	}
	// Cascade to our direct successor to continue deleting replicas
	// around the ring.
	node.mu.Lock()
	succ := node.successorList[0]
	node.mu.Unlock()
	if succ != "" && succ != node.Addr {
		node.RemoteCall(succ, "ChordNode.DeleteReplica", key, &struct{}{})
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

// Notify is called by a node that thinks it might be our predecessor.
func (node *ChordNode) Notify(args string, _ *struct{}) error {
	node.mu.Lock()

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
			// Pull keys that hash into (predecessor, self] from successor.
			var migrated map[string]string
			r := Range{Start: toID(args), End: node.ID}
			if err := node.RemoteCall(succ, "ChordNode.TransferKeys", r, &migrated); err == nil {
				node.dataLock.Lock()
				for k, v := range migrated {
					node.data[k] = v
				}
				node.dataLock.Unlock()
			}

			// Return local keys that do NOT belong to (predecessor, self].
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
