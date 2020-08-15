package chord

import (
	"errors"
	"fmt"
	"math/big"
	"net/rpc"
	"sync"
	"time"
)

const (
	MaxM        = 160
	ReqZero     = 0
	MaxReqTimes = 5
)

type EdgeType struct {
	IP string
	ID *big.Int
}

type FindType struct {
	ID  *big.Int
	cnt int
}

type MapWithLock struct {
	Map  map[string]string
	lock sync.Mutex
}

type Node struct {
	IP          string
	ID          *big.Int
	FingerTable [MaxM + 1]EdgeType
	Successors  [MaxM + 1]EdgeType
	sLock       sync.Mutex
	Predecessor *EdgeType
	Data        MapWithLock
	backup      MapWithLock
	Connected   bool
	next        int
}

type KVPair struct {
	Key, Val string
}

// go func
func (n *Node) Stabilize() {
	for n.Connected {
		n.Stabilize_()
		time.Sleep(233 * time.Millisecond)
	}
}

func (n *Node) Stabilize_() {
	var suc EdgeType
	suc = n.GetWorkingSuc()
	if suc.IP == "" {
		return
	}
	client, err := rpc.Dial("tcp", suc.IP)
	if err != nil { // Dial failed
		return
	}

	var pre EdgeType
	err = client.Call("NetNode.GetPre", ReqZero, &pre)
	if err != nil || pre.IP == "" {
		err = client.Call("NetNode.Notify", &EdgeType{n.IP, n.ID}, nil)
		if err != nil { // Call failed
			return
		}
		var sucList [MaxM + 1]EdgeType
		err = client.Call("NetNode.GetSucList", ReqZero, &sucList)
		if err != nil { // Call failed
			return
		}
		n.sLock.Lock()
		for i := 2; i <= MaxM; i++ {
			n.Successors[i] = sucList[i-1]
		}
		n.sLock.Unlock()
		_ = client.Close()
		return
	}
	if pre.IP != "" && n.Ping(pre.IP) {
		n.sLock.Lock()
		if between(n.ID, pre.ID, n.Successors[1].ID, false) {
			n.Successors[1] = pre
		}
		_ = client.Close()
		client, err = rpc.Dial("tcp", n.Successors[1].IP)
		if err == nil {
			defer func() { _ = client.Close() }()
		}
		n.sLock.Unlock()
		if err != nil { // Dial failed
			return
		}
		err = client.Call("NetNode.Notify", &EdgeType{n.IP, n.ID}, nil)
		if err != nil { // Call failed
			return
		}
		var sucList [MaxM + 1]EdgeType
		err = client.Call("NetNode.GetSucList", ReqZero, &sucList)
		if err != nil { // Call failed
			return
		}
		n.sLock.Lock()
		for i := 2; i <= MaxM; i++ {
			n.Successors[i] = sucList[i-1]
		}
		n.sLock.Unlock()
	}
}

func (n *Node) Notify(pre *EdgeType, _ *int) error {
	if n.Predecessor == nil || between(n.Predecessor.ID, pre.ID, n.ID, false) {
		n.Predecessor = pre
		client, err := rpc.Dial("tcp", n.Predecessor.IP)
		if err != nil { // Dial failed
			return err
		}
		defer func() {
			_ = client.Close()
		}()
		preMap := make(map[string]string)
		err = client.Call("NetNode.GetDataMap", ReqZero, &preMap)
		n.backup.lock.Lock()
		n.backup.Map = preMap
		n.backup.lock.Unlock()
		return nil
	}
	if n.Predecessor.IP == pre.IP || n.Predecessor.IP == n.IP {
		return nil
	}
	return errors.New("node.go, notify(): notify failed")
	//return nil
}

func (n *Node) Init(str string) {
	n.IP = LocAddr() + ":" + str
	n.ID = hash(n.IP)
	n.Predecessor = nil
	n.Data.Map = make(map[string]string)
	n.backup.Map = make(map[string]string)
	n.next = 1
}

func (n *Node) Create() {
	n.Predecessor = &EdgeType{n.IP, new(big.Int).Set(n.ID)}
	for i := 1; i <= MaxM; i++ {
		n.Successors[i] = EdgeType{n.IP, new(big.Int).Set(n.ID)}
	}
}

// put <K, V>
func (n *Node) Put(key, val string) bool {
	keyID := hash(key)
	var suc EdgeType
	err := n.FindSuc(&FindType{new(big.Int).Set(keyID), 0}, &suc)
	if err != nil {
		fmt.Println(err)
		return false
	}
	client, err := rpc.Dial("tcp", suc.IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil { // Dial failed
		fmt.Println(err)
		return false
	}

	var done bool
	err = client.Call("NetNode.InsertVal", KVPair{key, val}, &done)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return done
}

// look up K for V; return "" if not found
func (n *Node) Get(key string) (bool, string) {
	var done = false
	var val = ""

	keyID := hash(key)
	var suc EdgeType
	for i := 0; !done && i < MaxReqTimes; i++ {
		err := n.FindSuc(&FindType{new(big.Int).Set(keyID), 0}, &suc)
		if err != nil {
			fmt.Println(err)
			return false, val
		}
		client, err := rpc.Dial("tcp", suc.IP)
		if err != nil { // Dial failed
			time.Sleep(503 * time.Millisecond)
		} else {
			err = client.Call("NetNode.LookupKey", key, &val)
			_ = client.Close()
			if err == nil {
				done = true
				break
			}
		}
	}
	if val == "" {
		return false, ""
	}
	if done {
		return done, val
	} else {
		fmt.Println("more than max_req_times")
		return done, val
	}
}

// delete <K, V>; do nothing if K is not found
func (n *Node) Delete(key string) bool {
	keyID := hash(key)
	var suc EdgeType
	err := n.FindSuc(&FindType{new(big.Int).Set(keyID), 0}, &suc)
	if err != nil {
		fmt.Println(err)
		return false
	}
	client, err := rpc.Dial("tcp", suc.IP)
	if err != nil {
		fmt.Println(err)
		return false
	}
	err = client.Call("NetNode.DeleteKey", key, nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	_ = client.Close()
	return true
}

func (n *Node) Dump() {
	fmt.Println("IP:   ", n.IP)
	fmt.Println("ID:   ", n.ID)
	if n.Predecessor != nil {
		fmt.Println("pre:  ", n.Predecessor.IP, "pre < self?", n.Predecessor.ID.Cmp(n.ID))
	} else {
		fmt.Println("pre:  nil")
	}
	fmt.Println("suc1: ", n.Successors[1].IP, "self < suc1?", n.ID.Cmp(n.Successors[1].ID))
	fmt.Println("suc2: ", n.Successors[2].IP, "suc1 < suc2?", n.Successors[1].ID.Cmp(n.Successors[2].ID))
	fmt.Println("finger0: ", n.FingerTable[0].IP)
	fmt.Println("finger1: ", n.FingerTable[1].IP)
	fmt.Println("is on:", n.Connected)
	fmt.Println(n.Data.Map["Rouge"])
}

func (n *Node) Join(IP string) bool {
	client, err := rpc.Dial("tcp", IP)
	if err != nil {
		fmt.Println(err)
		return false
	}
	n.Predecessor = nil

	err = client.Call("NetNode.FindSuc", &FindType{new(big.Int).Set(n.ID), 0}, &n.Successors[1])
	if err != nil {
		fmt.Println(err)
		return false
	}
	_ = client.Close()

	client, err = rpc.Dial("tcp", n.GetWorkingSuc().IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		fmt.Println(err)
		return false
	}
	var pre EdgeType
	err = client.Call("NetNode.GetPre", ReqZero, &pre)
	if err != nil || pre.IP == "" {
		fmt.Println(err)
		return false
	}
	var sucMap map[string]string
	err = client.Call("NetNode.GetDataMap", ReqZero, &sucMap)
	if err != nil {
		fmt.Println(err)
		return false
	}

	var sucList [MaxM + 1]EdgeType
	err = client.Call("NetNode.GetSucList", ReqZero, &sucList)
	n.sLock.Lock()
	for i := 2; i <= MaxM; i++ {
		n.Successors[i] = sucList[i-1]
	}
	n.sLock.Unlock()

	n.Data.lock.Lock()
	for key, val := range sucMap {
		if between(pre.ID, hash(key), n.ID, true) {
			n.Data.Map[key] = val
		}
	}
	n.Data.lock.Unlock()

	// remove some from n.suc.data.map
	err = client.Call("NetNode.JoinSucRemove", &EdgeType{n.IP, n.ID}, nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	// fix n.suc.backup
	err = client.Call("NetNode.Notify", &EdgeType{n.IP, n.ID}, nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	go n.Stabilize()
	go n.FixFinger()
	go n.CheckPre()
	n.Connected = true
	return true
}

func (n *Node) Quit() {
	_ = n.FixSuc()
	if n.GetWorkingSuc().IP == n.IP {
		n.Connected = false
		return
	}

	// move all n.data to n.suc.data
	client, err := rpc.Dial("tcp", n.GetWorkingSuc().IP)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = client.Call("NetNode.QuitMoveData", &n.Data, nil)
	err = client.Call("NetNode.QuitMoveDataPre", &n.Data, nil)
	if err != nil {
		fmt.Println(err)
		_ = client.Close()
		return
	}
	_ = client.Close()

	// fix n.pre.sucList
	if n.Predecessor == nil {
		fmt.Println("no pre found")
		return
	}
	client, err = rpc.Dial("tcp", n.Predecessor.IP)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = client.Call("NetNode.QuitFixPreSucList", n.GetWorkingSuc(), nil)
	if err != nil {
		fmt.Println(err)
		_ = client.Close()
		return
	}
	_ = client.Close()

	// fix n.suc.pre
	client, err = rpc.Dial("tcp", n.GetWorkingSuc().IP)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = client.Call("NetNode.QuitFixSucPre", n.Predecessor, nil)
	if err != nil {
		fmt.Println(err)
		_ = client.Close()
		return
	}

	err = n.FixSuc()
	if err != nil {
		fmt.Println(err)
		_ = client.Close()
		return
	}
	_ = client.Close()

	n.Connected = false
}
