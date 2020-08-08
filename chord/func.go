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

// func (t *T) MethodName(request T1,response *T2) error

// check whether elt is between start and end
// if inclusive == true, it tests if elt is in (start, end]
// otherwise it tests if elt is in (start, end)
func between (left, x, right *big.Int, equalRight bool) bool {
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

//for reference:
//func _between(start, elt, end *big.Int, inclusive bool) bool {
//	if end.Cmp(start) > 0 {
//		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
//	} else {
//		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
//	}
//}

func (n *Node) getWorkingSuc() EdgeType {
	for i := 0; i < MaxM; i++ {
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
			case done = <- ch:
				if done {
					return true
				} else {
					continue
				}
			case <- time.After(503 * time.Millisecond):
				continue
		}
	}
	return false
}

//n.fix_finger()
//n.finger[next]  = n.find_succ(n.id + 2next)	next = next + 1
//if next > 159
//next = 0
//n.finger is  a array with length of 160, and here we use C-Style indexing
//find_succ() is a function that find which node the specific key belonged to.


//n.check_predecessor()
//if n.pre is failed
//n.pre = nil

//we can use function like ping to check whether the predecessor is failed