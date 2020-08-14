package chord

import "net"

type NetNode struct {
	Info   *Node
	Listen net.Listener
}

func (n *NetNode) GetDataMap(_ int, ret *map[string]string) error {return n.Info.GetDataMap(ReqZero, ret)}

func (n *NetNode) GetPre(_ int, ret *EdgeType) error {return n.Info.GetPre(ReqZero, ret)}

func (n *NetNode) GetSucList(_ int, ret *[MaxM + 1]EdgeType) error {return n.Info.GetSucList(ReqZero, ret)}

func (n *NetNode) Notify(pre *EdgeType, _ *int) error {return n.Info.Notify(pre, nil)}

func (n *NetNode) InsertVal(req KVPair, done *bool) error {return n.Info.InsertVal(req, done)}

func (n *NetNode) PutValBackup(req KVPair, done *bool) error {return n.Info.PutValBackup(req, done)}

func (n *NetNode) LookupKey(key string, val *string) error {return n.Info.LookupKey(key, val)}

func (n *NetNode) LookupKeyBackup(key string, val *string) error {return n.Info.LookupKeyBackup(key, val)}

func (n *NetNode) DeleteKey(key string, _ *int) error {return n.Info.DeleteKey(key, nil)}

func (n *NetNode) DeleteKeyBackup(key string, _ *int) error {return n.Info.DeleteKeyBackup(key, nil)}

func (n *NetNode) JoinSucRemove(suc EdgeType, _ *int) error {return n.Info.JoinSucRemove(suc, nil)}

func (n *NetNode) QuitFixSucPre(pre EdgeType, _ *int) error {return n.Info.QuitFixSucPre(pre, nil)}

func (n *NetNode) QuitFixPreSucList(suc EdgeType, _ *int) error {return n.Info.QuitFixPreSucList(suc, nil)}

func (n *NetNode) QuitMoveData(ret *MapWithLock, _ *int) error {return n.Info.QuitMoveData(ret, nil)}

func (n *NetNode) QuitMoveDataPre(ret *MapWithLock, _ *int) error {return n.Info.QuitMoveDataPre(ret, nil)}

func (n *NetNode) FindSuc(req *FindType, ans *EdgeType) error {return n.Info.FindSuc(req, ans)}

/*
for reference:

errCall := client.Call("Node.getPre", ReqZero, &pre)
errCall = client.Call("Node.getSucList", ReqZero, &sucList)
errCall = client.Call("Node.notify", &EdgeType{n.IP, n.ID}, nil)
err = client.Call("Node.getDataMap", ReqZero, &preMap)
err = client.Call("Node.insertVal", KVPair{key, val}, &done)
err = client.Call("Node.lookupKey", key, &val)
err = client.Call("Node.deleteKey", key, nil)
//err = client.Call("Node.getPre", ReqZero, &pre)
err = client.Call("Node.getDataMap", ReqZero, &sucMap)
//err = client.Call("Node.getSucList", ReqZero, &sucList)
err = client.Call("Node.joinSucRemove", &EdgeType{n.IP, n.ID}, nil)
//err = client.Call("Node.notify", &EdgeType{n.IP, n.ID}, nil)
err = client.Call("Node.quitFixSucPre", n.Predecessor, nil)
err = client.Call("Node.quitFixPreSucList", n.Successors[1], nil)
//err = client.Call("Node.quitFixSucPre", n.Predecessor, nil)

err = client.Call("Node.findSuc", req, ans)
err = client.Call("Node.putValBackup", req, done)
err = client.Call("Node.deleteKeyBackup", key, nil)
err = client.Call("Node.getSucList", ReqZero, &sucList)
err = client.Call("Node.putValBackup", KVPair{key, val}, nil)
 */
