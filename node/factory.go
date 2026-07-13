package node

// default test

/*
func NewNode(port int) DhtNode {
	node := new(Node)
	node.Init(portToAddr(localAddress, port))
	return node
}*/

// Chord test

/*
func NewNode(port int) DhtNode {
	node := new(ChordNode)
	node.Init(portToAddr(localAddress, port))
	return node
}*/

// Kademlia test

func NewNode(port int) DhtNode {
	node := new(KademliaNode)
	node.Init(portToAddr(localAddress, port))
	return node
}
