package kademlia

import (
	"net"
	"net/rpc"

	"github.com/sirupsen/logrus"
)

// PING RPC method for checking node availability.
func (node *KademliaNode) Ping(_ string, _ *struct{}) error {
	return nil
}

// StoreArgs definition
type StoreArgs struct {
	Key   string
	Value string
}

// STORE RPC method for storing data in the DHT.
func (node *KademliaNode) Store(args *StoreArgs, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[args.Key] = args.Value
	node.dataLock.Unlock()
	return nil
}

// FIND_NODE RPC method for retrieving data from the DHT.
func (node *KademliaNode) FindNode(args *[IDLength]byte, reply *[]Contact) error {
	*reply = node.findClosestContacts(*args, K)
	return nil
}

// FindValueReply definition
type FindValueReply struct {
	Value    string
	Contacts []Contact
	Found    bool
}

// FIND_VALUE RPC method for retrieving data from the DHT.
func (node *KademliaNode) FindValue(key string, reply *FindValueReply) error {
	node.dataLock.RLock()
	value, exists := node.data[key]
	node.dataLock.RUnlock()

	if exists {
		reply.Value = value
		reply.Found = true
	} else {
		hashKey := hash(key)
		reply.Contacts = node.findClosestContacts(hashKey, K)
		reply.Found = false
	}
	return nil
}

// RemoteCall performs an RPC call to a remote node.
func (node *KademliaNode) RemoteCall(addr, method string, args interface{}, reply interface{}) error {
	if method != "KademliaNode.Ping" && method != "KademliaNode.Introduce" {
		logrus.Infof("[%s] RemoteCall %s %s", node.Addr, addr, method)
	}
	conn, err := net.DialTimeout("tcp", addr, RPCTimeout)
	if err != nil {
		logrus.Error("dialing: ", err)
		node.removeContact(addr)
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Error("RemoteCall error: ", err)
		return err
	}

	// Update the routing table with gathered info about the node
	node.updateRoutingTable(addr)

	// Introduce ourselves to the remote node so it also learns about us.
	// Best-effort; ignore failure.
	client.Call("KademliaNode.Introduce", node.Addr, &struct{}{})
	return nil
}

// rawCall performs an RPC call WITHOUT the Introduce step, to avoid
// cascading routing-table updates from internal liveness checks.
// It does NOT evict contacts on failure — the caller (e.g. insertContact)
// is responsible for handling unresponsive contacts.
func (node *KademliaNode) rawCall(addr, method string, args interface{}, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", addr, RPCTimeout)
	if err != nil {
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}

// INTRODUCE RPC: lets the receiver learn the caller's Kademlia address.
func (node *KademliaNode) Introduce(callerAddr string, _ *struct{}) error {
	node.updateRoutingTable(callerAddr)
	return nil
}

// Additional RPC: GET_ALL_DATA
func (node *KademliaNode) GetAllData(_ string, reply *map[string]string) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()

	*reply = make(map[string]string)
	for key, value := range node.data {
		(*reply)[key] = value
	}
	return nil
}

// Additional RPC: DELETE_DATA
func (node *KademliaNode) DeleteData(key string, reply *bool) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()

	if _, exists := node.data[key]; exists {
		delete(node.data, key)
		*reply = true
	} else {
		*reply = false
	}
	return nil
}

// Stops an RPC server gracefully
func (node *KademliaNode) stopRPCServer() {
	node.online = false
	close(node.shutdown)
	if node.listener != nil {
		node.listener.Close()
	}
}
