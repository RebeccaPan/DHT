package chord

type NetNode struct {
	Info Node
}

func (n *NetNode) getDataMap(_ int, ret *map[string]string) error {return n.n.getDataMap(ReqZero, ret)}

func (n *NetNode) getPre(_ int, ret *EdgeType) error {return n.n.getPre(ReqZero, ret)}

func (n *NetNode) getSucList(_ int, ret *[MaxM + 1]EdgeType) error {return n.n.getSucList(ReqZero, ret)}

func (n *NetNode) notify(pre *EdgeType, _ *int) error {return n.n.notify(pre, nil)}

func (n *NetNode) insertVal(req KVPair, done *bool) error {return n.n.insertVal(req, done)}

func (n *NetNode) lookupKey(key string, val *string) error {return n.n.lookupKey(key, val)}

func (n *NetNode) deleteKey(key string, _ int) error {return n.n.deleteKey(key, ReqZero)}

func (n *NetNode) joinSucRemove(suc EdgeType, _ int) error {return n.n.joinSucRemove(suc, ReqZero)}

func (n *NetNode) quitFixSucPre(pre EdgeType, _ int) error {return n.n.quitFixSucPre(pre, ReqZero)}

func (n *NetNode) quitFixPreSucList(suc EdgeType, _ int) error {return n.n.quitFixPreSucList(suc, ReqZero)}

func (n *NetNode) findSuc(req *FindType, ans *EdgeType) error {return n.n.findSuc(req, ans)}

func (n *NetNode) putValBackup(req KVPair, done *bool) error {return n.n.putValBackup(req, done)}

func (n *NetNode) deleteKeyBackup(key string, _ int) error {return n.n.deleteKeyBackup(key, ReqZero)}

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
