package node

// default test

/*
func NewNode(port int) DhtNode {
	node := new(Node)
	node.Init(portToAddr(localAddress, port))
	return node
}
*/

// Chord test
/*
import "dht/chord"
func NewNode(port int) DhtNode {
	node := new(chord.ChordNode)
	node.Init(portToAddr(localAddress, port))
	return node
}
*/
// Kademlia test

import "dht/kademlia"
func NewNode(port int) DhtNode {
	node := new(kademlia.KademliaNode)
	node.Init(portToAddr(localAddress, port))
	return node
}


