package kademlia

import (
	"math/rand"
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const K = 7
const ALPHA = 3
const M = 160

type KademliaEntry struct {
	Addr string
	Id   *big.Int
}

type dataEntry struct {
	Value   string
	Version int64
}

type KBucket struct {
	entries []*KademliaEntry
}

type KademliaNode struct {
	Addr   string
	online atomic.Bool
	id     *big.Int

	data       map[string]dataEntry
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
	Version  int64
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
	Version    int64
	SenderAddr string
}

type PingArgs struct {
	SenderAddr string
}

type DeleteKeyArgs struct {
	Key        string
	Version    int64
	SenderAddr string
}

type DeleteKeyReply struct {
	Deleted bool
	Nodes   []KademliaEntry
}

type Pair struct {
	Key   string
	Value string
}

func hash(s string) *big.Int {
	res := sha1.Sum([]byte(s))
	return new(big.Int).SetBytes(res[:])
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
	if node.id.Cmp(entry.Id) == 0 {
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
			return
		}
	}
	if len(bucket.entries) < K {
		bucket.entries = append(bucket.entries, entry)
		node.bucketLock.Unlock()
		return
	}
	tmp := bucket.entries[0]
	tmpAddr := tmp.Addr
	node.bucketLock.Unlock()
	err := node.RemoteCall(tmpAddr, "KademliaNode.Ping", &PingArgs{node.Addr}, &struct{}{})
	node.bucketLock.Lock()
	bucket = node.buckets[idx]
	if len(bucket.entries) == 0 || bucket.entries[0].Addr != tmpAddr {
		node.bucketLock.Unlock()
		return
	}
	for _, now := range bucket.entries {
		if now.Id.Cmp(entry.Id) == 0 {
			node.bucketLock.Unlock()
			return
		}
	}
	if err != nil {
		bucket.entries = append(bucket.entries[1:], entry)
	}
	node.bucketLock.Unlock()
}

func (node *KademliaNode) removeFromBucket(addr string) {
	node.bucketLock.Lock()
	defer node.bucketLock.Unlock()
	idx := bucketIndex(node.id, hash(addr))
	if idx < 0 {
		return
	}
	bucket := node.buckets[idx]
	for i, tmp := range bucket.entries {
		if tmp.Addr == addr {
			bucket.entries = append(bucket.entries[:i], bucket.entries[i+1:]...)
			return
		}
	}
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
				array = append(array, pair{tmp, new(big.Int).Xor(tmp.Id, targetID)})
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

func (node *KademliaNode) Init(addr string) {
	node.Addr = addr
	node.id = hash(addr)
	node.data = make(map[string]dataEntry)
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
				err := node.RemoteCall(addr, "KademliaNode.FindNode", &FindNodeArgs{targetID, node.Addr}, &reply)
				if err != nil {
					node.removeFromBucket(addr)
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
				if !flag {
					shortlist = append(shortlist, tmp)
				}
			}
		}
		sort.Slice(shortlist, func(i, j int) bool {
			return new(big.Int).Xor(shortlist[i].Id, targetID).Cmp(
				new(big.Int).Xor(shortlist[j].Id, targetID)) < 0
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
		ver   int64
		nodes []KademliaEntry
	}
	keyID := hash(key)
	node.dataLock.RLock()
	if val, ok := node.data[key]; ok {
		node.dataLock.RUnlock()
		return true, val.Value
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
				err := node.RemoteCall(addr, "KademliaNode.FindValue", &FindValueArgs{key, node.Addr}, &reply)
				if err != nil {
					node.removeFromBucket(addr)
					return
				}
				res[idx].flag = reply.HasValue
				res[idx].value = reply.Value
				res[idx].ver = reply.Version
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
				cacheTargets := node.findNode(keyID)
				for _, entry := range cacheTargets {
					err := node.RemoteCall(entry.Addr, "KademliaNode.Store",
						&StoreArgs{key, foundValue, res[i].ver, node.Addr}, &struct{}{})
					if err != nil {
						node.removeFromBucket(entry.Addr)
					}
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
				if !flag {
					shortlist = append(shortlist, tmp)
				}
			}
		}
		sort.Slice(shortlist, func(i, j int) bool {
			return new(big.Int).Xor(shortlist[i].Id, keyID).Cmp(
				new(big.Int).Xor(shortlist[j].Id, keyID)) < 0
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
	entry, ok := node.data[args.Key]
	node.dataLock.RUnlock()
	if ok {
		reply.HasValue = true
		reply.Value = entry.Value
		reply.Version = entry.Version
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
	entry, ok := node.data[args.Key]
	if !ok || args.Version > entry.Version {
		node.data[args.Key] = dataEntry{args.Value, args.Version}
	}
	node.dataLock.Unlock()
	return nil
}

func (node *KademliaNode) Ping(args *PingArgs, _ *struct{}) error {
	node.addToBucket(&KademliaEntry{args.SenderAddr, hash(args.SenderAddr)})
	return nil
}

func (node *KademliaNode) DeleteKey(args *DeleteKeyArgs, reply *DeleteKeyReply) error {
	node.addToBucket(&KademliaEntry{args.SenderAddr, hash(args.SenderAddr)})
	node.dataLock.Lock()
	entry, ok := node.data[args.Key]
	if ok && args.Version >= entry.Version {
		delete(node.data, args.Key)
		node.dataLock.Unlock()
		reply.Deleted = true
	} else {
		node.dataLock.Unlock()
		reply.Deleted = false
	}
	keyID := hash(args.Key)
	reply.Nodes = node.findClosest(keyID, K)
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
			time.Sleep(5 * time.Second)
			randID := hash(fmt.Sprintf("%d", rand.Int()))
			node.findNode(randID)
		}
	}()
}

func (node *KademliaNode) Create() {}

func (node *KademliaNode) Join(addr string) bool {
	err := node.RemoteCall(addr, "KademliaNode.Ping", &PingArgs{node.Addr}, &struct{}{})
	if err != nil {
		return false
	}
	node.addToBucket(&KademliaEntry{addr, hash(addr)})
	node.findNode(node.id)
	return true
}

func (node *KademliaNode) Put(key string, value string) bool {
	ver := time.Now().UnixNano()
	keyID := hash(key)
	targets := node.findNode(keyID)
	var ok atomic.Bool
	ok.Store(false)
	var wg sync.WaitGroup
	for _, tmp := range targets {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			err := node.RemoteCall(addr, "KademliaNode.Store", &StoreArgs{key, value, ver, node.Addr}, &struct{}{})
			if err == nil {
				ok.Store(true)
			} else {
				node.removeFromBucket(addr)
			}
		}(tmp.Addr)
	}
	wg.Wait()
	return ok.Load()
}

func (node *KademliaNode) Get(key string) (bool, string) {
	return node.findValue(key)
}

func (node *KademliaNode) Delete(key string) bool {
	deleteVersion := time.Now().UnixNano()
	keyID := hash(key)
	deleted := false
	node.dataLock.Lock()
	if entry, ok := node.data[key]; ok && deleteVersion >= entry.Version {
		delete(node.data, key)
		deleted = true
	}
	node.dataLock.Unlock()
	shortlist := node.findClosest(keyID, K)
	if len(shortlist) == 0 {
		return deleted
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
		res := make([]DeleteKeyReply, len(array))
		for i, tmp := range array {
			wg.Add(1)
			go func(idx int, addr string) {
				defer wg.Done()
				var reply DeleteKeyReply
				err := node.RemoteCall(addr, "KademliaNode.DeleteKey",
					&DeleteKeyArgs{key, deleteVersion, node.Addr}, &reply)
				if err != nil {
					node.removeFromBucket(addr)
					return
				}
				res[idx] = reply
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
			if res[i].Deleted {
				deleted = true
			}
		}
		for i := 0; i < len(res); i++ {
			for _, tmp := range res[i].Nodes {
				flag := false
				for _, entry := range shortlist {
					if entry.Id.Cmp(tmp.Id) == 0 {
						flag = true
						break
					}
				}
				if !flag {
					shortlist = append(shortlist, tmp)
				}
			}
		}
		sort.Slice(shortlist, func(i, j int) bool {
			return new(big.Int).Xor(shortlist[i].Id, keyID).Cmp(new(big.Int).Xor(shortlist[j].Id, keyID)) < 0
		})
		done := true
		for _, tmp := range shortlist {
			if !visited[tmp.Addr] {
				done = false
				break
			}
		}
		if done {
			break
		}
	}
	return deleted
}

func (node *KademliaNode) Quit() {
	node.dataLock.Lock()
	pairs := make([]Pair, 0, len(node.data))
	versions := make([]int64, 0, len(node.data))
	for k, v := range node.data {
		pairs = append(pairs, Pair{k, v.Value})
		versions = append(versions, v.Version)
	}
	node.dataLock.Unlock()
	for i, pair := range pairs {
		keyID := hash(pair.Key)
		targets := node.findClosest(keyID, K)
		key, value := pair.Key, pair.Value
		ver := versions[i]
		var wg sync.WaitGroup
		for _, t := range targets {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				err := node.RemoteCall(addr, "KademliaNode.Store", &StoreArgs{key, value, ver, node.Addr}, &struct{}{})
				if err != nil {
					node.removeFromBucket(addr)
				}
			}(t.Addr)
		}
		wg.Wait()
	}
	node.StopRPCServer()
}

func (node *KademliaNode) ForceQuit() {
	node.StopRPCServer()
}
