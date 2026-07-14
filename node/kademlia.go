package node

import (
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const K = 4
const ALPHA = 2

// const M = 160

type KademliaEntry struct {
	Addr string
	Id   *big.Int
}

type KBucket struct {
	entries []*KademliaEntry
}

type KademliaNode struct {
	Addr   string
	online atomic.Bool
	id     *big.Int

	data       map[string]string
	dataLock   sync.RWMutex
	buckets    [M]*KBucket
	bucketLock sync.RWMutex

	listener net.Listener
	server   *rpc.Server
}

type FindValueReply struct {
	Value    string
	Nodes    []KademliaEntry
	HasValue bool
}

type FindNodeArgs struct {
	TargetID   *big.Int
	SenderAddr string
}

type FindValueArgs struct {
	Key        string
	SenderAddr string
}

type StoreArgs struct {
	Key        string
	Value      string
	SenderAddr string
}

type PingArgs struct {
	SenderAddr string
}

func xorDistance(a, b *big.Int) *big.Int {
	res := new(big.Int).Xor(a, b)
	return res
}

func bucketIndex(nodeID, targetID *big.Int) int {
	tmp := new(big.Int).Xor(nodeID, targetID)
	if tmp.Sign() == 0 {
		return -1
	} else {
		return tmp.BitLen() - 1
	}
}

func (node *KademliaNode) addToBucket(entry *KademliaEntry) {
	logrus.Infof("addToBucket %s", entry.Addr)
	if node.id.Cmp(entry.Id) == 0 {
		logrus.Infof("fail #1 %s", entry.Addr)
		return
	}
	node.bucketLock.Lock()
	idx := bucketIndex(node.id, entry.Id)
	bucket := node.buckets[idx]
	for i, now := range bucket.entries {
		if now.Id.Cmp(entry.Id) == 0 {
			bucket.entries = append(bucket.entries[:i], bucket.entries[i+1:]...)
			bucket.entries = append(bucket.entries, entry)
			node.bucketLock.Unlock()
			logrus.Infof("fail #2 %s", entry.Addr)
			return
		}
	}
	if len(bucket.entries) < K {
		bucket.entries = append(bucket.entries, entry)
		node.bucketLock.Unlock()
		logrus.Infof("fail #3 %s", entry.Addr)
		return
	}
	tmp := bucket.entries[0]
	tmpAddr := tmp.Addr
	node.bucketLock.Unlock()
	err := node.RemoteCall(tmpAddr, "KademliaNode.Ping", &PingArgs{SenderAddr: node.Addr}, &struct{}{})
	node.bucketLock.Lock()
	bucket = node.buckets[idx]
	if len(bucket.entries) == 0 || bucket.entries[0].Addr != tmpAddr {
		node.bucketLock.Unlock()
		logrus.Infof("fail #4 %s", entry.Addr)
		return
	}
	if err != nil {
		bucket.entries = append(bucket.entries[1:], entry)
	}
	node.bucketLock.Unlock()
	logrus.Infof("fail #5 %s", entry.Addr)
}

func (node *KademliaNode) removeFromBucket(addr string) {
}

func (node *KademliaNode) findClosest(targetID *big.Int, count int) []KademliaEntry {
	type pair struct {
		entry *KademliaEntry
		dis   *big.Int
	}
	node.bucketLock.RLock()
	defer node.bucketLock.RUnlock()
	var array []pair
	for _, bucket := range node.buckets {
		for _, tmp := range bucket.entries {
			if tmp.Id.Cmp(node.id) != 0 {
				array = append(array, pair{tmp, xorDistance(tmp.Id, targetID)})
			}
		}
	}
	sort.Slice(array, func(i, j int) bool { return array[i].dis.Cmp(array[j].dis) < 0 })
	if count > len(array) {
		count = len(array)
	}
	res := make([]KademliaEntry, count)
	for i := 0; i < count; i++ {
		res[i] = *array[i].entry
	}
	return res
}

// Initialize a node.

func (node *KademliaNode) Init(addr string) {
	node.Addr = addr
	node.id = hash(addr)
	node.data = make(map[string]string)
	for i := 0; i < M; i++ {
		node.buckets[i] = &KBucket{}
	}
}

func (node *KademliaNode) RunRPCServer(wg *sync.WaitGroup) {
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

func (node *KademliaNode) StopRPCServer() {
	node.online.Store(false)
	node.listener.Close()
}

func (node *KademliaNode) findNode(targetID *big.Int) []KademliaEntry {
	shortlist := node.findClosest(targetID, K)
	if len(shortlist) == 0 {
		return nil
	}
	visited := make(map[string]bool)
	visited[node.Addr] = true
	for {
		var array []KademliaEntry
		for _, tmp := range shortlist {
			if visited[tmp.Addr] == false {
				array = append(array, tmp)
			}
			if len(array) == ALPHA {
				break
			}
		}
		if len(array) == 0 {
			break
		}

		var wg sync.WaitGroup
		res := make([][]KademliaEntry, len(array))
		for i, tmp := range array {
			wg.Add(1)
			go func(idx int, addr string) {
				defer wg.Done()
				var reply []KademliaEntry
				err := node.RemoteCall(addr, "KademliaNode.FindNode", &FindNodeArgs{TargetID: targetID, SenderAddr: node.Addr}, &reply)
				if err != nil {
					return
				}
				res[idx] = reply
				for _, entry := range reply {
					node.addToBucket(&KademliaEntry{entry.Addr, entry.Id})
				}
			}(i, tmp.Addr)
		}
		wg.Wait()

		for _, tmp := range array {
			visited[tmp.Addr] = true
		}
		for i := 0; i < len(res); i++ {
			for _, tmp := range res[i] {
				flag := false
				for _, entry := range shortlist {
					if entry.Id.Cmp(tmp.Id) == 0 {
						flag = true
						break
					}
				}
				if !flag && node.id.Cmp(tmp.Id) != 0 {
					shortlist = append(shortlist, tmp)
				}
			}
		}
		sort.Slice(shortlist, func(i, j int) bool {
			return xorDistance(shortlist[i].Id, targetID).Cmp(
				xorDistance(shortlist[j].Id, targetID)) < 0
		})
		if len(shortlist) > K {
			shortlist = shortlist[:K]
		}
		flag := true
		for i := 0; i < len(shortlist) && i < K; i++ {
			if !visited[shortlist[i].Addr] {
				flag = false
				break
			}
		}
		if flag {
			break
		}
	}
	return shortlist
}

func (node *KademliaNode) findValue(key string) (bool, string) {
	type result struct {
		flag  bool
		value string
		nodes []KademliaEntry
	}
	keyID := hash(key)
	node.dataLock.RLock()
	if val, ok := node.data[key]; ok {
		node.dataLock.RUnlock()
		return true, val
	}
	node.dataLock.RUnlock()

	shortlist := node.findClosest(keyID, K)
	if len(shortlist) == 0 {
		return false, ""
	}
	visited := make(map[string]bool)
	visited[node.Addr] = true
	for {
		var array []KademliaEntry
		for _, tmp := range shortlist {
			if !visited[tmp.Addr] {
				array = append(array, tmp)
			}
			if len(array) == ALPHA {
				break
			}
		}
		if len(array) == 0 {
			break
		}
		var wg sync.WaitGroup
		res := make([]result, len(array))
		for i, tmp := range array {
			wg.Add(1)
			go func(idx int, addr string) {
				defer wg.Done()
				var reply FindValueReply
				err := node.RemoteCall(addr, "KademliaNode.FindValue", &FindValueArgs{Key: key, SenderAddr: node.Addr}, &reply)
				if err != nil {
					return
				}
				res[idx].flag = reply.HasValue
				res[idx].value = reply.Value
				res[idx].nodes = reply.Nodes
				for _, entry := range reply.Nodes {
					node.addToBucket(&KademliaEntry{entry.Addr, entry.Id})
				}
			}(i, tmp.Addr)
		}
		wg.Wait()

		for _, tmp := range array {
			visited[tmp.Addr] = true
		}
		for i := 0; i < len(res); i++ {
			if res[i].flag {
				foundValue := res[i].value
				// Store the value back to the K nearest nodes
				cacheTargets := node.findClosest(keyID, K)
				for _, entry := range cacheTargets {
				node.RemoteCall(entry.Addr, "KademliaNode.Store",
					&StoreArgs{Key: key, Value: foundValue, SenderAddr: node.Addr}, &struct{}{})
				}
				return true, foundValue
			}
		}
		for i := 0; i < len(res); i++ {
			for _, tmp := range res[i].nodes {
				flag := false
				for _, entry := range shortlist {
					if entry.Id.Cmp(tmp.Id) == 0 {
						flag = true
						break
					}
				}
				if !flag && node.id.Cmp(tmp.Id) != 0 {
					shortlist = append(shortlist, tmp)
				}
			}
		}
		sort.Slice(shortlist, func(i, j int) bool {
			return xorDistance(shortlist[i].Id, keyID).Cmp(
				xorDistance(shortlist[j].Id, keyID)) < 0
		})
		if len(shortlist) > K {
			shortlist = shortlist[:K]
		}
		flag := true
		for i := 0; i < len(shortlist) && i < K; i++ {
			if !visited[shortlist[i].Addr] {
				flag = false
				break
			}
		}
		if flag {
			break
		}
	}
	return false, ""
}

//
// RPC Methods
//

func (node *KademliaNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "KademliaNode.Ping" {
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

func (node *KademliaNode) FindNode(args *FindNodeArgs, reply *[]KademliaEntry) error {
	node.addToBucket(&KademliaEntry{args.SenderAddr, hash(args.SenderAddr)})
	*reply = node.findClosest(args.TargetID, K)
	return nil
}

func (node *KademliaNode) FindValue(args *FindValueArgs, reply *FindValueReply) error {
	node.addToBucket(&KademliaEntry{args.SenderAddr, hash(args.SenderAddr)})
	node.dataLock.RLock()
	value, ok := node.data[args.Key]
	node.dataLock.RUnlock()
	if ok {
		reply.HasValue = true
		reply.Value = value
		return nil
	}
	reply.HasValue = false
	keyID := hash(args.Key)
	reply.Nodes = node.findClosest(keyID, K)
	return nil
}

func (node *KademliaNode) Store(args *StoreArgs, _ *struct{}) error {
	node.addToBucket(&KademliaEntry{args.SenderAddr, hash(args.SenderAddr)})
	node.dataLock.Lock()
	node.data[args.Key] = args.Value
	node.dataLock.Unlock()
	return nil
}

func (node *KademliaNode) Ping(args *PingArgs, _ *struct{}) error {
	node.addToBucket(&KademliaEntry{args.SenderAddr, hash(args.SenderAddr)})
	return nil
}

func (node *KademliaNode) DeleteData(key string, reply *bool) error {
	return nil
}

//
// DHT methods
//

func (node *KademliaNode) Run(wg *sync.WaitGroup) {
	node.online.Store(true)
	go node.RunRPCServer(wg)
	go func() {
		for node.online.Load() {
			time.Sleep(time.Second)
			randID := hash(fmt.Sprintf("%d", rand.Int()))
			node.findNode(randID)
		}
	}()
	go func() {
		for node.online.Load() {
			time.Sleep(5000 * time.Millisecond)
			node.dataLock.Lock()
			pairs := make([]Pair, 0, len(node.data))
			for k, v := range node.data {
				pairs = append(pairs, Pair{k, v})
			}
			node.dataLock.Unlock()
			for _, pair := range pairs {
				node.Put(pair.Key, pair.Value)
			}
		}
	}()
}

func (node *KademliaNode) Create() {}

func (node *KademliaNode) Join(addr string) bool {
	logrus.Infof("Join %s", addr)
	err := node.RemoteCall(addr, "KademliaNode.Ping", &PingArgs{SenderAddr: node.Addr}, &struct{}{})
	if err != nil {
		return false
	}
	node.addToBucket(&KademliaEntry{addr, hash(addr)})
	node.findNode(node.id)
	return true
}

func (node *KademliaNode) Put(key string, value string) bool {
	logrus.Infof("Put %s %s", key, value)
	keyID := hash(key)
	targets := node.findNode(keyID)
	var ok atomic.Bool
	ok.Store(false)
	var wg sync.WaitGroup
	for _, tmp := range targets {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			err := node.RemoteCall(addr, "KademliaNode.Store", &StoreArgs{Key: key, Value: value, SenderAddr: node.Addr}, &struct{}{})
			if err == nil {
				ok.Store(true)
			}
		}(tmp.Addr)
	}
	wg.Wait()
	node.dataLock.Lock()
	node.data[key] = value
	node.dataLock.Unlock()
	return ok.Load()
}

func (node *KademliaNode) Get(key string) (bool, string) {
	logrus.Infof("Get %s", key)
	return node.findValue(key)
}

func (node *KademliaNode) Delete(key string) bool {
	return true
}

func (node *KademliaNode) Quit() {
	logrus.Infof("Quit %s", node.Addr)
	node.StopRPCServer()
	node.dataLock.Lock()
	pairs := make([]Pair, 0, len(node.data))
	for k, v := range node.data {
		pairs = append(pairs, Pair{k, v})
	}
	node.dataLock.Unlock()
	for _, pair := range pairs {
		node.Put(pair.Key, pair.Value)
	}
	logrus.Infof("Finish Quit %s", node.Addr)
}

func (node *KademliaNode) ForceQuit() {
	logrus.Infof("ForceQuit %s", node.Addr)
	node.StopRPCServer()
}

func (node *KademliaNode) DeBug() {
	logrus.Infof("DeBug %s %s", node.Addr, node.id)
	for i := 0; i < M; i++ {
		if len(node.buckets[i].entries) > 0 {
			for _, tmp := range node.buckets[i].entries {
				logrus.Infof("Find %v %s", i, tmp.Id)
			}
		}
	}
}
