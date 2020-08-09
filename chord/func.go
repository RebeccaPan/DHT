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

// calc n + 2^next
func jump(n *big.Int, next int) *big.Int {
	var two = big.NewInt(2)
	var hashMod = new(big.Int).Exp(two, big.NewInt(sha1.Size*8), nil)

	pow := new(big.Int).Exp(two, big.NewInt(int64(next)), nil)
	ans := new(big.Int).Add(n, pow)
	return new(big.Int).Mod(ans, hashMod)
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

func (n *Node) findSuc(req *FindType, ans *EdgeType) error {
	req.cnt += 1
	if req.cnt > MaxReqTimes {
		return errors.New("func.go, findSuc(): not found when looking up")
	}
	suc := n.getWorkingSuc()
	if suc.IP == "" {
		return errors.New("func.go, findSuc(): cannot get working suc")
	}
	if req.ID.Cmp(n.ID) == 0 || suc.ID.Cmp(n.ID) == 0 {
		*ans = EdgeType{n.IP, new(big.Int).Set(n.ID)}
		return nil
	}
	if between(n.ID, req.ID, suc.ID, true) {
		*ans = EdgeType{suc.IP, new(big.Int).Set(suc.ID)}
		return nil
	}
	nxt := n.closestPreNode(req.ID)
	if nxt.IP == "" {
		return errors.New("func.go, findSuc(): cannot find closest pre node")
	}
	client, err := rpc.Dial("tcp", nxt.IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		time.Sleep(1000 * time.Millisecond)
		return n.findSuc(req, ans)
	}
	err = client.Call("Node.findSuc", req, ans)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) closestPreNode(reqID *big.Int) EdgeType {
	for i := MaxM; i >= 1; i-- {
		if n.FingerTable[i].ID != nil && n.ping(n.FingerTable[i].IP) && between(n.ID, n.FingerTable[i].ID, reqID, true) {
			return n.FingerTable[i]
		}
	}
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
	if n.Successors[1].IP == n.IP { // only one node on chord
		return nil
	}
	n.sLock.Lock()
	index := 1
	var found = false
	for index = 1; index <= MaxM; index++ {
		if n.ping(n.Successors[index].IP) {
			found = true
			break
		}
	}
	if !found || index == 1 {
		n.sLock.Unlock()
		return errors.New("func.go, fixSuc(): no working suc found")
	}
	n.Successors[1] = n.Successors[index]
	n.sLock.Unlock()

	client, err := rpc.Dial("tcp", n.Successors[1].IP)
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

func (n *Node) fixFinger() {
	n.next = 1
	var find FindType
	for n.Connected {
		if n.Successors[1].IP != n.FingerTable[1].IP || n.Successors[1].ID != n.FingerTable[1].ID {
			n.next = 1
		}
		for i := 0; i < MaxReqTimes; i++ {
			find.ID = jump(n.ID, n.next)
			find.cnt = 0
			err := n.findSuc(&find, &n.FingerTable[n.next])
			if err != nil {
				time.Sleep(503 * time.Millisecond)
			} else {
				break
			}
		}
		cur := n.FingerTable[n.next]
		n.next++
		if n.next > MaxM {
			n.next = 1
		} else {
			for {
				if !between(n.ID, jump(n.ID, n.next), cur.ID, true) {
					break
				}
				n.FingerTable[n.next] = EdgeType{cur.IP, new(big.Int).Set(cur.ID)}
				n.next++
				if n.next > MaxM {
					n.next = 1
					break
				}
			}
		}
		time.Sleep(233 * time.Millisecond)
	}

}

func (n *Node) checkPre() bool {
	if !n.ping(n.Predecessor.IP) {
		n.Predecessor = nil
		return false
	}
	return true
}
