package kademlia

import (
	"net"
	"net/rpc"
	"time"

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
	*reply = node.findNode(*args)
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
		reply.Contacts = node.findNode(hashKey)
		reply.Found = false
	}
	return nil
}

// RemoteCall performs an RPC call to a remote node.
func (node *KademliaNode) RemoteCall(addr, method string, args interface{}, reply interface{}) error {
	if method != "KademliaNode.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s", node.Addr, addr, method)
	}
	conn, err := net.DialTimeout("tcp", addr, RPCTimeout)
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

	// Update the routing table with gathered info about the node
	node.updateRoutingTable(addr)
	return nil
}

// Additional RPC: GET_ALL_DATA
func (node *KademliaNode) GetAllData(_ string, reply *[]Contact) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()

	*reply = []Contact{}
	for key, _ := range node.data {
		contact := Contact{
			NodeID:   hash(key),
			Addr:     node.Addr,
			LastSeen: time.Now(),
		}
		*reply = append(*reply, contact)
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
