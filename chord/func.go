package chord

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

func hash(str string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(str))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(two, big.NewInt(MaxM), nil)

// calc n + 2^(next-1), next = 1, 2, ...
func jump(n *big.Int, next int) *big.Int {
	pow := new(big.Int).Exp(two, big.NewInt(int64(next-1)), nil)
	ans := new(big.Int).Add(n, pow)
	return new(big.Int).Mod(ans, hashMod)
}

func between(left, x, right *big.Int, equalRight bool) bool {
	if right.Cmp(x) == 0 {
		return equalRight
	} else {
		if left.Cmp(right) < 0 {
			return left.Cmp(x) < 0 && x.Cmp(right) < 0
		} else { // take the chord into consideration
			return left.Cmp(x) < 0 || x.Cmp(right) < 0
		}
	}
}

func LocAddr() string {
	var str string
	Itf, err := net.Interfaces()
	if err != nil {
		panic("net.Interfaces not found")
	}
	// find the first non-loopback interface with an IP address
	for _, elt := range Itf {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrS, err := elt.Addrs()
			if err != nil {
				panic("failure to get addresses for net.Interfaces")
			}
			for _, addr := range addrS {
				if ipNet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipNet.IP.To4(); len(ip4) == net.IPv4len {
						str = ip4.String()
						break
					}
				}
			}
		}
	}
	if str == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return str
}

func (n *Node) GetWorkingSuc() EdgeType {
	for i := 1; i <= MaxM; i++ {
		if n.Ping(n.Successors[i].IP) {
			return n.Successors[i]
		}
	}
	return EdgeType{"", nil}
}

func (n *Node) GetPre(_ int, ret *EdgeType) error {
	if n.Predecessor != nil {
		*ret = EdgeType{n.Predecessor.IP, new(big.Int).Set(n.Predecessor.ID)}
		return nil
	} else {
		return errors.New("func.go, GetPre(): no pre found")
	}
}

func (n *Node) GetDataMap(_ int, ret *map[string]string) error {
	n.Data.lock.Lock()
	*ret = n.Data.Map
	n.Data.lock.Unlock()
	return nil
}

func (n *Node) GetSucList(_ int, ret *[MaxM + 1]EdgeType) error {
	n.sLock.Lock()
	for i := 1; i <= MaxM; i++ {
		if n.Successors[i].ID == nil {
			fmt.Println("GetSucList failure", n.ID)
		} else {
			(*ret)[i] = EdgeType{n.Successors[i].IP, new(big.Int).Set(n.Successors[i].ID)}
		}
	}
	n.sLock.Unlock()
	return nil
}

func (n *Node) FindSuc(req *FindType, ans *EdgeType) error {
	req.cnt += 1
	if req.cnt > MaxReqTimes {
		return errors.New("func.go, FindSuc(): not found when looking up")
	}
	_ = n.FixSuc()
	suc := n.GetWorkingSuc()
	if suc.IP == "" {
		return errors.New("func.go, FindSuc(): cannot get working suc")
	}
	if req.ID.Cmp(n.ID) == 0 || suc.ID.Cmp(n.ID) == 0 {
		*ans = EdgeType{n.IP, new(big.Int).Set(n.ID)}
		return nil
	}
	if between(n.ID, req.ID, suc.ID, true) {
		*ans = EdgeType{suc.IP, new(big.Int).Set(suc.ID)}
		return nil
	}
	nxt := n.ClosestPreNode(req.ID)
	if nxt.IP == "" {
		return errors.New("func.go, FindSuc(): cannot find closest pre node")
	}
	client, err := rpc.Dial("tcp", nxt.IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		time.Sleep(1000 * time.Millisecond)
		return n.FindSuc(req, ans)
	}
	err = client.Call("NetNode.FindSuc", req, ans)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) ClosestPreNode(reqID *big.Int) EdgeType {
	for i := MaxM; i >= 1; i-- {
		if n.FingerTable[i].ID != nil && n.Ping(n.FingerTable[i].IP) && between(n.ID, n.FingerTable[i].ID, reqID, true) {
			return EdgeType{n.FingerTable[i].IP, new(big.Int).Set(n.FingerTable[i].ID)}
		}
	}
	_ = n.FixSuc()
	if n.Ping(n.Successors[1].IP) {
		return EdgeType{n.Successors[1].IP, new(big.Int).Set(n.Successors[1].ID)}
	}
	return EdgeType{"", nil}
}

// when Put()
// insert val into this node && the backup of this node's suc
func (n *Node) InsertVal(req KVPair, done *bool) error {
	*done = false
	n.Data.lock.Lock()
	n.Data.Map[req.Key] = req.Val
	n.Data.lock.Unlock()

	_ = n.FixSuc()
	client, err := rpc.Dial("tcp", n.GetWorkingSuc().IP)
	if err != nil {
		return err
	}
	err = client.Call("NetNode.PutValBackup", req, done)
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
func (n *Node) PutValBackup(req KVPair, done *bool) error {
	*done = false
	n.backup.lock.Lock()
	n.backup.Map[req.Key] = req.Val
	n.backup.lock.Unlock()
	*done = true
	return nil
}

// when Get()
func (n *Node) LookupKey(key string, val *string) error {
	*val = ""
	n.Data.lock.Lock()
	*val = n.Data.Map[key]
	n.Data.lock.Unlock()
	if *val == "" {
		_ = n.FixSuc()
		client, err := rpc.Dial("tcp", n.GetWorkingSuc().IP)
		if err != nil {
			return err
		}
		_ = client.Call("NetNode.LookupKeyBackup", key, val)
		_ = client.Close()
	}
	if *val == "" {
		return errors.New("get failure: get empty string in map")
	}
	return nil
}
func (n *Node) LookupKeyBackup(key string, val *string) error {
	*val = ""
	n.backup.lock.Lock()
	*val = n.backup.Map[key]
	n.backup.lock.Unlock()
	return nil
}

// when Delete()
func (n *Node) DeleteKey(key string, _ *int) error {
	client, err := rpc.Dial("tcp", n.GetWorkingSuc().IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return err
	}
	err = client.Call("NetNode.DeleteKeyBackup", key, nil)
	if err != nil {
		return err
	}
	n.Data.lock.Lock()
	delete(n.Data.Map, key)
	n.Data.lock.Unlock()
	return nil
}
func (n *Node) DeleteKeyBackup(key string, _ *int) error {
	n.backup.lock.Lock()
	delete(n.backup.Map, key)
	n.backup.lock.Unlock()
	return nil
}

// when Join()
func (n *Node) JoinSucRemove(suc EdgeType, _ *int) error {
	n.Data.lock.Lock()
	var toDel []string
	for key := range n.Data.Map {
		if between(n.Predecessor.ID, hash(key), suc.ID, true) {
			toDel = append(toDel, key)
		}
	}
	for _, str := range toDel {
		delete(n.Data.Map, str)
	}
	n.Data.lock.Unlock()
	return nil
}

// when quit()
// set n.sucList as suc & suc.sucList
func (n *Node) QuitFixPreSucList(suc EdgeType, _ *int) error {
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
	err = client.Call("NetNode.GetSucList", ReqZero, &sucList)
	if err != nil {
		return err
	}
	n.sLock.Lock()
	for i := 2; i <= MaxM; i++ {
		n.Successors[i] = sucList[i-1]
	}
	n.sLock.Unlock()
	return nil
}

// when quit()
// set n.pre as pre
func (n *Node) QuitFixSucPre(pre EdgeType, _ *int) error {
	n.Predecessor = &pre
	return nil
}

// when quit()
// move req.map to n.data.map and fix n.backup.map
func (n *Node) QuitMoveData(req *MapWithLock, _ *int) error {
	client, err := rpc.Dial("tcp", n.Successors[1].IP)
	if err == nil {
		defer func() {
			_ = client.Close()
		}()
	}
	if err != nil {
		return err
	}

	n.Data.lock.Lock()
	req.lock.Lock()
	for key, val := range req.Map {
		n.Data.Map[key] = val
		err = client.Call("NetNode.PutValBackup", KVPair{key, val}, nil)
		if err != nil {
			n.Data.lock.Unlock()
			req.lock.Unlock()
			return err
		}
	}
	n.Data.lock.Unlock()
	req.lock.Unlock()
	return nil
}

func (n *Node) QuitMoveDataPre(req *MapWithLock, _ *int) error {
	n.backup.lock.Lock()
	n.backup.Map = (*req).Map
	n.backup.lock.Unlock()
	return nil
}

func (n *Node) Ping(IP string) bool {
	if IP == "" {
		return false
	}
	var done = false
	for i := 0; i < MaxReqTimes; i++ {
		ch := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", IP)
			if err == nil {
				ch <- true
				_ = client.Close()
			} else {
				ch <- false
			}
		}()
		select {
		case done = <-ch:
			if done {
				return true
			} else {
				continue
			}
		case <-time.After(503 * time.Millisecond):
			break
		}
	}
	return false
}

func (n *Node) FixSuc() error {
	if n.Successors[1].IP == n.IP { // only one node on chord
		return nil
	}
	n.sLock.Lock()
	var index int
	var found = false
	for index = 1; index <= MaxM; index++ {
		if n.Ping(n.Successors[index].IP) {
			found = true
			break
		}
	}
	if index == 1 {
		n.sLock.Unlock()
		return nil
	}
	if !found {
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
	err = client.Call("NetNode.GetSucList", ReqZero, &sucList)
	if err != nil {
		return err
	}
	n.sLock.Lock()
	for i := 2; i <= MaxM; i++ {
		n.Successors[i] = sucList[i-1]
	}
	n.sLock.Unlock()
	return nil
}

// go func
func (n *Node) FixFinger() {
	for n.Connected {
		err := n.FindSuc(&FindType{jump(n.ID, n.next), 0}, &n.FingerTable[n.next])
		if err == nil {
			n.next = n.next%MaxM + 1 //1 ~ MaxM
		}
		time.Sleep(103 * time.Millisecond)
	}
}

// go func
func (n *Node) CheckPre() {
	for n.Connected {
		if n.Predecessor != nil && !n.Ping(n.Predecessor.IP) {
			n.Predecessor = nil
			client, err := rpc.Dial("tcp", n.Successors[1].IP)
			if err != nil {
				time.Sleep(233 * time.Millisecond)
				continue
			}
			n.Data.lock.Lock()
			n.backup.lock.Lock()
			for key, val := range n.backup.Map {
				n.Data.Map[key] = val
				if n.IP != n.Successors[1].IP {
					err = client.Call("NetNode.PutValBackup", KVPair{key, val}, nil)
					if err != nil {
						break
					}
				}
			}
			n.backup.Map = make(map[string]string)
			n.Data.lock.Unlock()
			n.backup.lock.Unlock()
			_ = client.Close()
		}
		time.Sleep(233 * time.Millisecond)
	}
}
