package node

import (
	"dht/chord"
)

func NewNode(port int) DhtNode {
	node := new(chord.ChordNode)
	node.Init(portToAddr(localAddress, port))
	return node
}
