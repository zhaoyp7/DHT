package node

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const M = 160
const SUC_LIST_LEN = 3

var ringSize = new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil)

type ChordEntry struct {
	Addr string
	Id   *big.Int
}

type ChordNode struct {
	Addr   string
	online atomic.Bool
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
	for node.online.Load() {
		conn, err := node.listener.Accept()
		if err != nil {
			logrus.Error("accept error: ", err)
			return
		}
		go node.server.ServeConn(conn)
	}
}

func (node *ChordNode) StopRPCServer() {
	node.online.Store(false)
	node.listener.Close()
}

func (node *ChordNode) stabilize() {
	defer node.updateSuccessorList()
	node.ringLock.RLock()
	suc := node.successor
	if suc == nil {
		node.ringLock.RUnlock()
		return
	}
	sucAddr := suc.Addr
	node.ringLock.RUnlock()
	err := node.RemoteCall(sucAddr, "ChordNode.Ping", "", &struct{}{})
	if err != nil {
		liveSuc := node.findFirstLiveSuccessor()
		if liveSuc != nil {
			node.ringLock.Lock()
			node.successor = liveSuc
			node.ringLock.Unlock()
		}
		return
	}
	var pre ChordEntry
	err = node.RemoteCall(sucAddr, "ChordNode.FindPredecessor", &struct{}{}, &pre)
	if err != nil {
		return
	}
	if pre.Id != nil && between(pre.Id, node.id, suc.Id) {
		err := node.RemoteCall(pre.Addr, "ChordNode.Ping", "", &struct{}{})
		if err == nil {
			node.ringLock.Lock()
			node.successor = &ChordEntry{pre.Addr, pre.Id}
			node.ringLock.Unlock()
		} else {
			node.RemoteCall(sucAddr, "ChordNode.SetPredecessor", &ChordEntry{node.Addr, node.id}, &struct{}{})
		}
	}
	node.ringLock.RLock()
	suc = node.successor
	sucAddr = suc.Addr
	node.ringLock.RUnlock()
	node.RemoteCall(sucAddr, "ChordNode.Notify", &ChordEntry{node.Addr, node.id}, &struct{}{})
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
	suc := node.successor
	nprime := node.closestPrecedingFinger(id)
	node.ringLock.RUnlock()
	if suc != nil && betweenRightClose(id, node.id, suc.Id) {
		return suc
	}
	if nprime.Id.Cmp(node.id) == 0 {
		if suc != nil && suc.Id.Cmp(node.id) != 0 {
			nprime = suc
		} else {
			return &ChordEntry{node.Addr, node.id}
		}
	}
	var result ChordEntry
	err := node.RemoteCall(nprime.Addr, "ChordNode.FindSuccessor", id, &result)
	if err != nil {
		err = node.RemoteCall(suc.Addr, "ChordNode.FindSuccessor", id, &result)
	}
	if err != nil {
		return &ChordEntry{node.Addr, node.id}
	} else {
		return &result
	}
}

func (node *ChordNode) closestPrecedingFinger(id *big.Int) *ChordEntry {
	for i := M - 1; i >= 0; i-- {
		if node.finger[i] != nil && between(node.finger[i].Id, node.id, id) {
			return node.finger[i]
		}
	}
	return &ChordEntry{node.Addr, node.id}
}

func (node *ChordNode) findFirstLiveSuccessor() *ChordEntry {
	node.ringLock.RLock()
	tmp := make([]*ChordEntry, len(node.successorList))
	copy(tmp, node.successorList)
	node.ringLock.RUnlock()
	for _, suc := range tmp {
		if suc != nil && node.id.Cmp(suc.Id) != 0 {
			err := node.RemoteCall(suc.Addr, "ChordNode.Ping", "", &struct{}{})
			if err == nil {
				return suc
			}
		}
	}
	node.ringLock.RLock()
	fingers := make([]*ChordEntry, len(node.finger))
	copy(fingers, node.finger)
	node.ringLock.RUnlock()
	for _, f := range fingers {
		if f != nil && node.id.Cmp(f.Id) != 0 {
			err := node.RemoteCall(f.Addr, "ChordNode.Ping", "", &struct{}{})
			if err == nil {
				return f
			}
		}
	}
	return nil
}

// my_list = [my_successor] + my_successor.successorList
func (node *ChordNode) updateSuccessorList() {
	suc := node.findFirstLiveSuccessor()
	if suc == nil {
		return
	}
	newList := []*ChordEntry{suc}
	sucList := make([]ChordEntry, 0, SUC_LIST_LEN)
	node.RemoteCall(suc.Addr, "ChordNode.FindSuccessors", &struct{}{}, &sucList)
	for i := 0; i < len(sucList) && i+1 < SUC_LIST_LEN; i++ {
		newList = append(newList, &ChordEntry{sucList[i].Addr, sucList[i].Id})
	}
	node.ringLock.Lock()
	node.successorList = newList
	node.ringLock.Unlock()
}

func (node *ChordNode) pushCopies() {
	node.ringLock.RLock()
	pre := node.predecessor
	sucList := make([]*ChordEntry, len(node.successorList))
	copy(sucList, node.successorList)
	node.ringLock.RUnlock()
	if pre == nil {
		return
	}
	dataSet := make([]Pair, 0)
	node.dataLock.RLock()
	for key, value := range node.data {
		if betweenRightClose(hash(key), pre.Id, node.id) {
			dataSet = append(dataSet, Pair{key, value})
		}
	}
	node.dataLock.RUnlock()
	for _, suc := range sucList {
		if suc == nil || suc.Id.Cmp(node.id) == 0 {
			continue
		}
		for _, tmp := range dataSet {
			go func(addr string, pair Pair) {
				node.RemoteCall(addr, "ChordNode.PutData", pair, nil)
			}(suc.Addr, tmp)
		}
	}
}

//
// RPC Methods
//

func (node *ChordNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "ChordNode.Ping" {
		logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr, addr, method, args)
	}
	// Note: Here we use DialTimeout to set a timeout of 10 seconds.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Errorf("[%s] dialing: %s", node.Addr, err)
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
	node.ringLock.RLock()
	defer node.ringLock.RUnlock()
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
	// logrus.Infof("enter FindSuccessors")
	node.ringLock.RLock()
	defer node.ringLock.RUnlock()
	res := make([]ChordEntry, 0, SUC_LIST_LEN)
	for i := 0; i < len(node.successorList); i++ {
		if node.successorList[i] != nil {
			res = append(res, *node.successorList[i])
		}
	}
	*reply = res
	// logrus.Infof("leave FindSuccessors")
	return nil
}

func (node *ChordNode) SetPredecessor(pre *ChordEntry, _ *struct{}) error {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	node.predecessor = pre
	return nil
}

func (node *ChordNode) SetSuccessor(suc *ChordEntry, _ *struct{}) error {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	node.successor = suc
	return nil
}

func (node *ChordNode) Ping(_ string, _ *struct{}) error {
	return nil
}

//
// DHT methods
//

func (node *ChordNode) Run(wg *sync.WaitGroup) {
	node.online.Store(true)
	go node.RunRPCServer(wg)
	go func() {
		for node.online.Load() {
			time.Sleep(200 * time.Millisecond)
			node.stabilize()
		}
	}()
	go func() {
		for node.online.Load() {
			time.Sleep(50 * time.Millisecond)
			node.fixFingers()
		}
	}()
	go func() {
		for node.online.Load() {
			time.Sleep(500 * time.Millisecond)
			node.pushCopies()
		}
	}()
}

func (node *ChordNode) Create() {
	node.ringLock.Lock()
	defer node.ringLock.Unlock()
	node.predecessor = nil
	node.successor = &ChordEntry{node.Addr, node.id}
	node.successorList = []*ChordEntry{node.successor}
}

func (node *ChordNode) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	node.ringLock.Lock()
	node.predecessor = nil
	node.ringLock.Unlock()
	var suc ChordEntry
	err := node.RemoteCall(addr, "ChordNode.FindSuccessor", node.id, &suc)
	if err != nil {
		return false
	}
	node.ringLock.Lock()
	node.successor = &ChordEntry{suc.Addr, suc.Id}
	node.successorList = []*ChordEntry{node.successor}
	node.ringLock.Unlock()
	node.dataLock.Lock()
	var result []Pair
	node.ringLock.RLock()
	sucAddr := node.successor.Addr
	sucID := node.successor.Id
	node.ringLock.RUnlock()
	node.RemoteCall(sucAddr, "ChordNode.TransferData", []*big.Int{node.id, sucID}, &result)
	for _, tmp := range result {
		node.data[tmp.Key] = tmp.Value
	}
	node.dataLock.Unlock()
	node.stabilize()
	return true
}

func (node *ChordNode) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	keyID := hash(key)
	target := node.findSuccessor(keyID)
	err := node.RemoteCall(target.Addr, "ChordNode.PutData", Pair{key, value}, nil)
	if err != nil {
		return false
	}
	var sucList []ChordEntry
	node.RemoteCall(target.Addr, "ChordNode.FindSuccessors", &struct{}{}, &sucList)
	for _, suc := range sucList {
		go func(addr string) {
			node.RemoteCall(addr, "ChordNode.PutData", Pair{key, value}, nil)
		}(suc.Addr)
	}
	return true
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
	// target := node.findSuccessor(keyID)
	var ok bool
	// node.RemoteCall(target.Addr, "ChordNode.DeleteData", key, &ok)
	now := new(big.Int).Set(keyID)
	for true {
		target := node.findSuccessor(now)
		var flag bool
		err := node.RemoteCall(target.Addr, "ChordNode.DeleteData", key, &flag)
		if err != nil || !flag {
			break
		}
		ok = true
		now = new(big.Int).Add(target.Id, big.NewInt(1))
		now = new(big.Int).Mod(now, ringSize)
	}
	return ok
}

func (node *ChordNode) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	node.StopRPCServer()
	node.ringLock.Lock()
	suc := node.successor
	pre := node.predecessor
	node.ringLock.Unlock()
	if suc == nil || node.id.Cmp(suc.Id) == 0 {
		suc = node.findFirstLiveSuccessor()
	} else {
		err := node.RemoteCall(suc.Addr, "ChordNode.Ping", "", &struct{}{})
		if err != nil {
			suc = node.findFirstLiveSuccessor()
		}
	}
	if suc != nil && node.id.Cmp(suc.Id) != 0 {
		node.dataLock.Lock()
		for key, value := range node.data {
			node.RemoteCall(suc.Addr, "ChordNode.PutData", Pair{key, value}, &struct{}{})
		}
		node.dataLock.Unlock()
	}
	logrus.Infof("Quit id = %s", node.id)
	if suc != nil {
		logrus.Infof("Quit suc = %s", suc.Id)
	}
	if pre != nil {
		logrus.Infof("Quit pre = %s", pre.Id)
	}
	if suc != nil && node.id.Cmp(suc.Id) != 0 && pre != nil {
		node.RemoteCall(suc.Addr, "ChordNode.SetPredecessor", pre, &struct{}{})
	}
	if pre != nil && suc != nil {
		node.RemoteCall(pre.Addr, "ChordNode.SetSuccessor", suc, &struct{}{})
	}
	logrus.Infof("Finish Quit %s", node.Addr)
}

func (node *ChordNode) ForceQuit() {
	logrus.Infof("ForceQuit %s", node.Addr)
	node.StopRPCServer()
}

func (node *ChordNode) DeBug() {
	node.ringLock.Lock()
	logrus.Infof("DeBug %s %s", node.Addr, node.id)
	logrus.Infof("suc is %s", node.successor.Id)
	logrus.Infof("pre is %s", node.predecessor.Id)
	node.ringLock.Unlock()
}
