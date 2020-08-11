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
	MaxReqTimes = 10
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
	Lock sync.Mutex
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
	if err == nil {
		defer func() {_ = client.Close()}()
	}
	if err != nil || client == nil { // Dial failed
		return
	}

	var pre EdgeType
	err = client.Call("NetNode.GetPre", ReqZero, &pre)
	//if err != nil || pre.IP == "" {
	//	return
	//}

	if err == nil && n.Ping(pre.IP) {
		n.sLock.Lock()
		if between(n.ID, pre.ID, n.Successors[1].ID, false) {
			n.Successors[1] = pre
		}
		client, err = rpc.Dial("tcp", n.Successors[1].IP)
		if err == nil {
			defer func() {_ = client.Close()}()
		}
		n.sLock.Unlock()
		if err != nil { // Dial failed
			return
		}
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
		n.backup.Lock.Lock()
		n.backup.Map = preMap
		n.backup.Lock.Unlock()
		return nil
	}
	if n.Predecessor.IP == pre.IP || n.Predecessor.IP == n.IP {
		return nil
	}
	return errors.New("node.go, notify(): notify failed")
	//return nil
}

func (n *Node) Init(str string) {
	// All to do when init
	n.IP = LocAddr() + ":" + str
	n.ID = hash(str)
	//n.FingerTable = nil
	//n.Successors  = nil
	n.Predecessor = nil
	n.Data.Map = make(map[string]string)
	n.backup.Map = make(map[string]string)
}

func (n *Node) Create() {
	n.Predecessor = &EdgeType{n.IP, new(big.Int).Set(n.ID)}
	for i := 1; i < MaxM; i++ {
		n.Successors[i] = EdgeType{n.IP, new(big.Int).Set(n.ID)}
	}
}

// put <K, V>
func (n *Node) Put(key, val string) bool {
	keyID := hash(key)
	var suc EdgeType
	err := n.FindSuc(&FindType{new(big.Int).Set(keyID), 0}, &suc)
	if err != nil {
		return false
	}
	client, err := rpc.Dial("tcp", suc.IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil || client == nil { // Dial failed
		return false
	}

	var done bool
	err = client.Call("NetNode.InsertVal", KVPair{key, val}, &done)
	if err != nil {
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
			return false, val
		}
		client, err := rpc.Dial("tcp", suc.IP)
		if err != nil {
			time.Sleep(503 * time.Millisecond)
		} else {
			err = client.Call("NetNode.LookupKey", key, &val)
			_ = client.Close()
			done = true
		}
	}
	return done, val
}

// delete <K, V>; do nothing if K is not found
func (n *Node) Delete(key string) bool {
	keyID := hash(key)
	var suc EdgeType
	err := n.FindSuc(&FindType{new(big.Int).Set(keyID), 0}, &suc)
	if err != nil {
		return false
	}
	client, err := rpc.Dial("tcp", suc.IP)
	if err != nil {
		return false
	}
	err = client.Call("NetNode.DeleteKey", key, nil)
	if err != nil {
		return false
	}
	_ = client.Close()
	return true
}

func (n *Node) Dump() {
	fmt.Println("IP:   ", n.IP)
	//fmt.Println("ID:   ", n.ID)
	if n.Predecessor != nil {
		fmt.Println("pre:  ", n.Predecessor.IP)
	} else {
		fmt.Println("pre:  nil")
	}
	fmt.Println("suc1: ", n.Successors[1].IP)
	fmt.Println("suc2: ", n.Successors[2].IP)
	fmt.Println("is on:", n.Connected)
}

func (n *Node) Join(IP string) bool {
	client, err := rpc.Dial("tcp", IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return false
	}
	n.Predecessor = nil

	err = client.Call("NetNode.FindSuc", &FindType{new(big.Int).Set(n.ID), 0}, &n.Successors[1])
	if err != nil {
		return false
	}

	client, err = rpc.Dial("tcp", n.GetWorkingSuc().IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return false
	}
	var pre EdgeType
	err = client.Call("NetNode.GetPre", ReqZero, &pre)
	if err != nil || pre.IP == "" {
		return false
	}
	var sucMap map[string]string
	err = client.Call("NetNode.GetDataMap", ReqZero, &sucMap)
	if err != nil {
		return false
	}

	var sucList [MaxM + 1]EdgeType
	err = client.Call("NetNode.GetSucList", ReqZero, &sucList)
	n.sLock.Lock()
	for i := 2; i < MaxM; i++ {
		n.Successors[i] = sucList[i-1]
	}
	n.sLock.Unlock()

	n.Data.Lock.Lock()
	for key, val := range sucMap {
		if !between(pre.ID, hash(key), n.ID, true) {
			n.Data.Map[key] = val
		}
	}
	n.Data.Lock.Unlock()

	// remove some from n.suc.data.map
	err = client.Call("NetNode.JoinSucRemove", &EdgeType{n.IP, n.ID}, nil)
	if err != nil {
		return false
	}
	// fix n.suc.backup
	err = client.Call("NetNode.Notify", &EdgeType{n.IP, n.ID}, nil)
	if err != nil {
		return false
	}
	go n.Stabilize()
	go n.FixFinger()
	go n.CheckPre()
	n.Connected = true
	return true
}

func (n *Node) Quit() {
	// fix n.pre.sucList
	client, err := rpc.Dial("tcp", n.Predecessor.IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return
	}
	err = client.Call("NetNode.QuitFixPreSucList", n.Successors[1], nil)
	if err != nil {
		return
	}

	// fix n.suc.pre
	client, err = rpc.Dial("tcp", n.Successors[1].IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return
	}
	err = client.Call("NetNode.QuitFixSucPre", n.Predecessor, nil)
	if err != nil {
		return
	}

	err = n.FixSuc()
	if err != nil {
		return
	}
	// move all n.data to n.suc.data
	client, err = rpc.Dial("tcp", n.Successors[1].IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return
	}
	err = client.Call("NetNode.QuitMoveData", &n.Data, nil)
	err = client.Call("NetNode.QuitMoveDataPre", &n.Data, nil)
	if err != nil {
		return
	}

	n.Connected = false
}
