package kademlia

import (
	"sync"

	"github.com/sirupsen/logrus"
)

// Put stores a key-value pair in the DHT
// Store the pair in the K-closest nodes from the target
func (node *KademliaNode) Put(key string, value string) bool {
	logrus.Infof("[%s] Putting key: %s, value: %s", node.Addr, key, value)

	keyID := hash(key)

	success := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	closest := node.findNode(keyID)

	for _, c := range closest {
		wg.Add(1)
		go func(contact Contact) {
			defer wg.Done()
			args := StoreArgs{
				Key:   key,
				Value: value,
			}
			err := node.RemoteCall(contact.Addr, "KademliaNode.Store", &args, &struct{}{})
			if err != nil {
				logrus.Errorf("[%s] Failed to store data on %s: %v", node.Addr, contact.Addr, err)
			} else {
				logrus.Infof("[%s] Successfully stored data on %s", node.Addr, contact.Addr)
				mu.Lock()
				success++
				mu.Unlock()
			}
		}(c)
	}

	wg.Wait()
	return success > 0
}

// Get retrieves a value from the DHT using the provided key
func (node *KademliaNode) Get(key string) (bool, string) {
	logrus.Infof("[%s] Getting key: %s", node.Addr, key)

	keyID := hash(key)

	node.dataLock.RLock()
	if val, ok := node.data[key]; ok {
		node.dataLock.RUnlock()
		return true, val
	}
	node.dataLock.RUnlock()

	shortlist := node.findClosestContacts(keyID, K)
	if len(shortlist) == 0 {
		return false, ""
	}

	queried := make(map[string]bool)

	for i := 0; i < 32; i++ {
		candidates := node.getAlphaClosestContacts(shortlist, keyID, queried)
		if len(candidates) == 0 {
			break
		}

		found := make(chan string, len(candidates))
		foundContacts := make(chan []Contact, len(candidates))
		var failedMu sync.Mutex
		failedAddrs := make(map[string]bool)
		var wg sync.WaitGroup

		for _, c := range candidates {
			queried[c.Addr] = true
			wg.Add(1)
			go func(contact Contact) {
				defer wg.Done()
				var reply FindValueReply
				err := node.RemoteCall(contact.Addr, "KademliaNode.FindValue", key, &reply)
				if err != nil {
					logrus.Errorf("[%s] Failed to find value on %s: %v", node.Addr, contact.Addr, err)
					failedMu.Lock()
					failedAddrs[contact.Addr] = true
					failedMu.Unlock()
					return
				}
				if reply.Found {
					found <- reply.Value
				} else {
					foundContacts <- reply.Contacts
				}
			}(c)
		}

		wg.Wait()
		close(found)
		close(foundContacts)

		// Remove dead contacts from shortlist so they don't cause
		// isLimitReached to stop the search prematurely.
		if len(failedAddrs) > 0 {
			alive := make([]Contact, 0, len(shortlist))
			for _, c := range shortlist {
				if !failedAddrs[c.Addr] {
					alive = append(alive, c)
				}
			}
			shortlist = alive
		}

		// Drain found values — a closed empty channel yields zero values,
		// so we must drain before deciding whether a value was found.
		foundValues := []string{}
		for v := range found {
			foundValues = append(foundValues, v)
		}
		if len(foundValues) > 0 {
			go node.cacheValue(key, foundValues[0], keyID)
			return true, foundValues[0]
		}

		for contacts := range foundContacts {
			for _, c := range contacts {
				if c.Addr != node.Addr {
					node.mergeShortlist(&shortlist, c, keyID, K)
				}
			}
		}

		if node.isLimitReached(shortlist, queried, keyID, K) {
			break
		}
	}

	return false, ""
}

// Cache values in the K closest nodes to the keyID
func (node *KademliaNode) cacheValue(key, value string, keyID [IDLength]byte) {
	closest := node.findNode(keyID)

	for _, c := range closest {
		if c.Addr != node.Addr {
			args := StoreArgs{
				Key:   key,
				Value: value,
			}
			err := node.RemoteCall(c.Addr, "KademliaNode.Store", &args, &struct{}{})
			if err != nil {
				logrus.Errorf("[%s] Failed to cache value on %s: %v", node.Addr, c.Addr, err)
			} else {
				logrus.Infof("[%s] Successfully cached value on %s", node.Addr, c.Addr)
			}
		}
	}
}

// Delete removes a key-value pair from the DHT.
// It deletes the key from the local node (if present) and from the K closest
// remote nodes. Returns true if at least one replica confirmed deletion.
func (node *KademliaNode) Delete(key string) bool {
	logrus.Infof("[%s] Deleting key: %s", node.Addr, key)

	keyID := hash(key)

	localDeleted := false
	node.dataLock.Lock()
	if _, exists := node.data[key]; exists {
		delete(node.data, key)
		localDeleted = true
	}
	node.dataLock.Unlock()

	closest := node.findNode(keyID)

	remoteDeleted := false
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, c := range closest {
		if c.Addr == node.Addr {
			continue // already handled locally
		}
		wg.Add(1)
		go func(contact Contact) {
			defer wg.Done()
			var deleted bool
			err := node.RemoteCall(contact.Addr,
				"KademliaNode.DeleteData", key, &deleted)
			if err != nil {
				logrus.Errorf("[%s] Failed to delete key on %s: %v",
					node.Addr, contact.Addr, err)
				return
			}
			if deleted {
				logrus.Infof("[%s] Successfully deleted key on %s",
					node.Addr, contact.Addr)
				mu.Lock()
				remoteDeleted = true
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	return localDeleted || remoteDeleted
}
