package node

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const M = 160

var ringSize = new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil)

type ChordEntry struct {
	Addr string
	Id   *big.Int
}

type ChordNode struct {
	Addr   string
	online bool
	id     *big.Int

	predecessor   *ChordEntry
	successor     *ChordEntry
	successorList []*ChordEntry
	finger        []*ChordEntry
	start         []*big.Int

	listener net.Listener
	server   *rpc.Server
	data     map[string]string
	dataLock sync.RWMutex

	ringLock sync.RWMutex
}

func hash(s string) *big.Int {
	res := sha1.Sum([]byte(s))
	return new(big.Int).SetBytes(res[:])
}

func between(id, a, b *big.Int) bool { // check whether id in (a, b)
	if a.Cmp(b) == 0 {
		return id.Cmp(a) != 0
	} else if a.Cmp(b) == -1 {
		return (id.Cmp(a) == 1 && id.Cmp(b) == -1)
	} else {
		return (id.Cmp(b) == -1 || id.Cmp(a) == 1)
	}
}

func betweenRightClose(id, a, b *big.Int) bool { // check whether id in (a, b]
	if a.Cmp(b) == 0 {
		return id.Cmp(a) != 0
	} else if a.Cmp(b) == -1 {
		return (id.Cmp(a) == 1 && id.Cmp(b) <= 0)
	} else {
		return (id.Cmp(b) <= 0 || id.Cmp(a) == 1)
	}
}

func betweenLeftClose(id, a, b *big.Int) bool { // check whether id in [a, b)
	if a.Cmp(b) == 0 {
		return true
	} else if a.Cmp(b) == -1 {
		return (id.Cmp(a) >= 0 && id.Cmp(b) == -1)
	} else {
		return (id.Cmp(b) == -1 || id.Cmp(a) >= 0)
	}
}

func computeStart(nodeID *big.Int, i uint) *big.Int {
	pow2i := new(big.Int).Lsh(big.NewInt(1), i)
	pow2M := new(big.Int).Lsh(big.NewInt(1), M)
	sum := new(big.Int).Add(nodeID, pow2i)
	result := new(big.Int).Mod(sum, pow2M)
	return result
}

// Initialize a node.

func (node *ChordNode) Init(addr string) {
	node.Addr = addr
	node.id = hash(addr)
	node.data = make(map[string]string)
	node.start = make([]*big.Int, M)
	node.finger = make([]*ChordEntry, M)
	for i := 0; i < M; i++ {
		node.start[i] = computeStart(node.id, uint(i))
	}
}

func (node *ChordNode) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)
	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatal("listen error: ", err)
	}
	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *ChordNode) StopRPCServer() {
	node.online = false
	node.listener.Close()
}

func (node *ChordNode) stabilize() {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	var pre ChordEntry
	succ := node.successor
	if succ == nil {
		return
	}
	err := node.RemoteCall(succ.Addr, "ChordNode.FindPredecessor", &struct{}{}, &pre)
	if err != nil {
		return
	}
	if pre.Id != nil && between(pre.Id, node.id, succ.Id) {
		node.successor = &pre
	}
	node.RemoteCall(node.successor.Addr, "ChordNode.Notify",
		&ChordEntry{Addr: node.Addr, Id: node.id}, &struct{}{})

}

func (node *ChordNode) fixFingers() {
	i := rand.Intn(M)
	node.ringLock.Lock()
	start := node.start[i]
	node.ringLock.Unlock()
	res := node.findSuccessor(start)
	node.ringLock.Lock()
	node.finger[i] = res
	node.ringLock.Unlock()
}

func (node *ChordNode) findSuccessor(id *big.Int) *ChordEntry {
	node.ringLock.RLock()
	succ := node.successor
	nprime := node.closestPrecedingFinger(id)
	node.ringLock.RUnlock()
	if succ != nil && betweenRightClose(id, node.id, succ.Id) {
		return succ
	}
	if nprime.Id.Cmp(node.id) == 0 {
		if succ != nil {
			return succ
		}
		return &ChordEntry{node.Addr, node.id}
	}
	var result ChordEntry
	err := node.RemoteCall(nprime.Addr, "ChordNode.FindSuccessor", id, &result)
	if err != nil {
		if succ != nil {
			return succ
		}
		return &ChordEntry{node.Addr, node.id}
	}
	return &result
}

func (node *ChordNode) closestPrecedingFinger(id *big.Int) *ChordEntry {
	for i := M - 1; i >= 0; i-- {
		if node.finger[i] != nil && between(node.finger[i].Id, node.id, id) {
			return node.finger[i]
		}
	}
	return &ChordEntry{node.Addr, node.id}
}

//
// RPC Methods
//

func (node *ChordNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "Node.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
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

func (node *ChordNode) Notify(entry *ChordEntry, _ *struct{}) error {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	if node.predecessor == nil || between(entry.Id, node.predecessor.Id, node.id) {
		node.predecessor = entry
	}
	return nil
}

func (node *ChordNode) FindSuccessor(id *big.Int, reply *ChordEntry) error {
	suc := node.findSuccessor(id)
	*reply = *suc
	return nil
}

func (node *ChordNode) FindPredecessor(_ *struct{}, reply *ChordEntry) error {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	if node.predecessor == nil {
		*reply = ChordEntry{}
		return nil
	}
	*reply = *node.predecessor

	return nil
}

func (node *ChordNode) TransferData(args []*big.Int, reply *[]Pair) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	st, ed := args[0], args[1]
	for key, value := range node.data {
		if !betweenRightClose(hash(key), st, ed) {
			*reply = append(*reply, Pair{key, value})
			delete(node.data, key)
		}
	}
	return nil
}

func (node *ChordNode) PutData(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	node.data[pair.Key] = pair.Value
	return nil
}

func (node *ChordNode) GetData(key string, reply *string) error {
	node.dataLock.RLock()
	defer node.dataLock.RUnlock()
	value, ok := node.data[key]
	if !ok {
		return fmt.Errorf("key %s not found", key)
	}
	*reply = value

	return nil
}

func (node *ChordNode) DeleteData(key string, reply *bool) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	_, ok := node.data[key]
	if !ok {
		*reply = false
		return nil
	}
	delete(node.data, key)
	*reply = true

	return nil
}

func (node *ChordNode) FindSuccessors(_ *struct{}, reply *[]ChordEntry) error {
	node.ringLock.RLock()
	defer node.ringLock.Unlock()
	if node.successor == nil {
		*reply = []ChordEntry{}
	} else {
		*reply = []ChordEntry{*node.successor}
	}
	return nil
}

//
// DHT methods
//

func (node *ChordNode) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
	go func() {
		for node.online {
			time.Sleep(200 * time.Millisecond)
			node.stabilize()
		}
	}()
	go func() {
		for node.online {
			time.Sleep(200 * time.Millisecond)
			node.fixFingers()
		}
	}()
}

func (node *ChordNode) Create() {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	node.predecessor = nil
	node.successor = &ChordEntry{node.Addr, node.id}
}

func (node *ChordNode) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	node.ringLock.Lock()
	node.predecessor = nil
	var suc ChordEntry
	err := node.RemoteCall(addr, "ChordNode.FindSuccessor", node.id, &suc)
	if err != nil {
		node.ringLock.Unlock()
		return false
	}
	node.successor = &ChordEntry{suc.Addr, suc.Id}
	node.ringLock.Unlock()
	node.dataLock.Lock()
	var result []Pair
	node.RemoteCall(suc.Addr, "ChordNode.TransferData", []*big.Int{node.id, suc.Id}, &result)
	for _, tmp := range result {
		node.data[tmp.Key] = tmp.Value
	}
	node.dataLock.Unlock()
	return true
}

func (node *ChordNode) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	keyID := hash(key)
	target := node.findSuccessor(keyID)
	err := node.RemoteCall(target.Addr, "ChordNode.PutData", Pair{key, value}, nil)
	return err == nil
}

func (node *ChordNode) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	keyID := hash(key)
	target := node.findSuccessor(keyID)
	var value string
	err := node.RemoteCall(target.Addr, "ChordNode.GetData", key, &value)
	if err != nil {
		return false, value
	}
	return true, value
}

func (node *ChordNode) Delete(key string) bool {
	logrus.Infof("Delete %s", key)
	keyID := hash(key)
	target := node.findSuccessor(keyID)
	var ok bool
	node.RemoteCall(target.Addr, "ChordNode.DeleteData", key, &ok)
	return ok
}

func (node *ChordNode) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	node.StopRPCServer()
}

func (node *ChordNode) ForceQuit() {
	logrus.Infof("ForceQuit %s", node.Addr)
	node.StopRPCServer()
}
