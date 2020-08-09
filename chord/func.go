package chord

import (
	"crypto/sha1"
	"errors"
	"math/big"
	"net/rpc"
	"time"
)

func hash(str string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(str))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

func between(left, x, right *big.Int, equalRight bool) bool {
	if right.Cmp(x) == 0 {
		return equalRight
	} else {
		if left.Cmp(right) < 0 {
			return left.Cmp(x) < 0 && x.Cmp(right) < 0
		} else {
			return left.Cmp(x) < 0 || x.Cmp(right) < 0
		}
	}
}

func (n *Node) getSuc() EdgeType {
	return n.Successors[1]
}

func (n *Node) getWorkingSuc() EdgeType {
	for i := 1; i < MaxM; i++ {
		if n.ping(n.Successors[i].IP) {
			return n.Successors[i]
		}
	}
	return EdgeType{"", nil}
}

func (n *Node) getPre(_ int, ret *EdgeType) error {
	if n.Predecessor != nil {
		ret = n.Predecessor
		return nil
	} else {
		return errors.New("func.go, getPre(): no pre found")
	}
}

func (n *Node) getDataMap(_ int, ret *map[string]string) error {
	n.Data.Lock.Lock()
	*ret = n.Data.Map
	n.Data.Lock.Unlock()
	return nil
}

func (n *Node) getSucList(_ int, ret *[MaxM + 1]EdgeType) error {
	n.sLock.Lock()
	for i := 0; i < MaxM; i++ {
		(*ret)[i] = EdgeType{n.Successors[i].IP, new(big.Int).Set(n.Successors[i].ID)}
	}
	n.sLock.Unlock()
	return nil
}

func (n *Node) findSuc(req *FindType, suc *EdgeType) error {
	// Todo: findSuc
	return nil
}

func (n *Node) closestPreNode(curID *big.Int) EdgeType {
	// Todo: closestPreNode
	return EdgeType{"", nil}
}

// when Put()
// insert val into this node && the backup of this node's suc
func (n *Node) insertVal(req KVPair, done *bool) error {
	*done = false
	n.Data.Lock.Lock()
	n.Data.Map[req.Key] = req.Val
	n.Data.Lock.Unlock()

	err := n.fixSuc()
	if err != nil {
		return err
	}
	client, err := rpc.Dial("tcp", n.getWorkingSuc().IP)
	if err != nil {
		return err
	}
	err = client.Call("Node.putValBackup", req, done)
	if err != nil {
		return err
	}
	err = client.Close()
	if err != nil {
		return err
	}
	*done = true
	return nil
}
func (n *Node) putValBackup(req KVPair, done *bool) error {
	n.backup.Lock.Lock()
	n.backup.Map[req.Key] = req.Val
	n.backup.Lock.Unlock()
	*done = true
	return nil
}

// when Get()
func (n *Node) lookupKey(key string, val *string) error {
	*val = ""
	n.Data.Lock.Lock()
	*val = n.Data.Map[key]
	n.Data.Lock.Unlock()
	return nil
}

// when Delete()
func (n *Node) deleteKey(key string, _ int) error {
	client, err := rpc.Dial("tcp", n.getWorkingSuc().IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return err
	}
	err = client.Call("Node.deleteKeyBackup", key, nil)
	if err != nil {
		return err
	}
	n.Data.Lock.Lock()
	delete(n.Data.Map, key)
	n.Data.Lock.Unlock()
	return nil
}
func (n *Node) deleteKeyBackup(key string, _ int) error {
	n.backup.Lock.Lock()
	delete(n.backup.Map, key)
	n.backup.Lock.Unlock()
	return nil
}

// when Join()
func (n *Node) joinSucOp(suc EdgeType, _ int) error {
	n.Data.Lock.Lock()
	var toDel []string
	for key := range n.Data.Map {
		if !between(n.Predecessor.ID, hash(key), suc.ID, true) {
			toDel = append(toDel, key)
		}
	}
	for _, str := range toDel {
		delete(n.Data.Map, str)
	}
	n.Data.Lock.Unlock()
	return nil
}

// when quit()
// set n.sucList as suc & suc.sucList
func (n *Node) quitFixPreSucList(suc EdgeType, _ int) error {
	n.Successors[1] = suc
	client, err := rpc.Dial("tcp", suc.IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return err
	}
	var sucList [MaxM + 1]EdgeType
	err = client.Call("Node.getSucList", ReqZero, &sucList)
	if err != nil {
		return err
	}
	n.sLock.Lock()
	for i := 2; i < MaxM; i++ {
		n.Successors[i] = sucList[i-1]
	}
	n.sLock.Unlock()
	return nil
}

// when quit()
// set n.pre as pre
func (n *Node) quitFixSucPre(pre EdgeType, _ int) error {
	n.Predecessor = &pre
	return nil
}

// when quit()
// move req.map to n.data.map and fix n.backup.map
func (n *Node) quitMoveData(req *MapWithLock, _ int) error {
	client, err := rpc.Dial("tcp", n.Successors[1].IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return err
	}

	n.Data.Lock.Lock()
	req.Lock.Lock()
	for key, val := range req.Map {
		n.Data.Map[key] = val
		err = client.Call("Node.putValBackup", KVPair{key, val}, nil)
		if err != nil {
			n.Data.Lock.Unlock()
			req.Lock.Unlock()
			return err
		}
	}
	n.Data.Lock.Unlock()
	req.Lock.Unlock()
	return nil
}

func (n *Node) quitMoveDataPre(req *MapWithLock, _ int) error {
	n.backup.Lock.Lock()
	n.backup.Map = (*req).Map
	n.backup.Lock.Unlock()
	return nil
}

func (n *Node) ping(IP string) bool {
	if IP == "" {
		return false
	}
	var done = false
	for i := 0; i < 3; i++ {
		ch := make(chan bool)
		go func() {
			_, err := rpc.Dial("tcp", IP)
			if err == nil {
				ch <- true
			} else {
				ch <- false
			}
			// Todo: more to do with recover() ??
		}()
		select {
		case done = <-ch:
			if done {
				return true
			} else {
				continue
			}
		case <-time.After(503 * time.Millisecond):
			continue
		}
	}
	return false
}

func (n *Node) fixSuc() error {
	// Todo: fixSuc()
	return nil
}

func (n *Node) fixFinger() error {
	// Todo: fixFinger()
	return nil
}

//n.fix_finger()
//n.finger[next]  = n.find_succ(n.id + 2next)	next = next + 1
//if next > 159
//next = 0
//n.finger is  a array with length of 160, and here we use C-Style indexing
//find_succ() is a function that find which node the specific key belonged to.

func (n *Node) checkPre() bool {
	if !n.ping(n.Predecessor.IP) {
		n.Predecessor = nil
		return false
	}
	return true
}

//n.check_predecessor()
//if n.pre is failed
//n.pre = nil

//we can use function like ping to check whether the predecessor is failed
