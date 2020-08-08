package chord

import (
	"errors"
	"math/big"
	"net/rpc"
	"sync"
)

const (
	MaxM    = 150
	reqZero = 0
)

type EdgeType struct {
	IP string
	ID *big.Int
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
	sLock		sync.Mutex
	Predecessor *EdgeType
	Data        MapWithLock
	backup		MapWithLock
}

type KVPair struct {
	Key, Val string
}

// functions that node can do

func (n *Node) stabilize() {
	var suc EdgeType
	suc = n.getWorkingSuc()
	if suc.IP == "" {
		return
	}
	client, errDial := rpc.Dial("tcp", suc.IP)
	if errDial == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if errDial != nil || client == nil { // Dial failed
		return
	}

	var pre EdgeType
	errCall := client.Call("Node.getPre", reqZero, &pre)
	if errCall != nil || pre.IP == "" {
		// Todo: what to do when err or pre cannot be pinged?
	}

	if errCall == nil && n.ping(pre.IP) {
		n.sLock.Lock()
		if between(n.ID, pre.ID, n.Successors[1].ID, false) {
			n.Successors[1] = pre
		}
		client, errDial = rpc.Dial("tcp", n.Successors[1].IP)
		n.sLock.Unlock()
		if errDial == nil {
			defer func() {
				_ = client.Close()
			}()
		}
		if errDial != nil { // Dial failed
			return
		}
	}
	var sucList [MaxM + 1]EdgeType
	errCall = client.Call("Node.getSucList", reqZero, &sucList)
	if errCall != nil { // Call failed
		return
	}
	n.sLock.Lock()
	for i := 2; i <= MaxM; i++ {
		n.Successors[i] = sucList[i-1]
	}
	n.sLock.Unlock()
	errCall = client.Call("Node.notify", &EdgeType{n.IP, n.ID}, nil)
	if errCall != nil { // Call failed
		return
	}
}

func (n *Node) notify(pre *EdgeType, _ *int) error {
	if n.Predecessor.IP == pre.IP {
		return nil
	}
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
		err = client.Call("Node.getKVMap", reqZero, &preMap)
		n.backup.Lock.Lock()
		n.backup.Map = preMap
		n.backup.Lock.Unlock()
		return nil
	}
	return errors.New("node.go, notify(): notify failed")
}

// put <K, V>
func (n *Node) Put(key, val string)  {
	// Todo
}

func (n *Node) LookUp(key string) (val string) {
	// look up K for V
}

func (n *Node) Remove(key, val string)  {
	// remove <K, V>
}

func (n *Node) Modify(key, newVal string)  {
	// modify <K, V> to <K, newV>
}

// functions for a node
func (n *Node) Init(str string)  {
	// All to do when init
	n.IP = str
	n.ID = hash(str)
	//n.FingerTable = nil
	//n.Successor   = nil
	n.Predecessor = nil
	//n.Data.Map
}

func (n *Node) Join(addr string)  {
	//n.join(n’)
	//n.pre = nil
	//n.succ = n’.find_predecessor()
	//for k, v in n.succ.data
	//if k is not between n and n.succ
	//n.data.append(k, v)
	//n.succ.data.remove(k)
}

func (n *Node) Quit() {
	//n.quit()
	//n.pre.succList.remove(n)
	//last := len(n.succList)
	//n.pre.succList.append(n.succList[len - 1])
	//n.succ.pre = n.pre
	//for k, v in n.data
	//n.succ.data.append(k ,v)
}