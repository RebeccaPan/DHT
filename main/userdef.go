package main

import (
	"chord"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	// Create a node and then return it.
	var node DHTNode
	node.Info = new(chord.NetNode)
	node.Info.Info = new(chord.Node)

	node.Port = strconv.Itoa(port)
	node.Server = rpc.NewServer()
	err := node.Server.Register(node.Info)
	if err != nil {
		// register failure
		return nil
	}
	node.Info.Info.Init(node.Port)
	return &node
}

// Implement a struct which implements the interface "dhtNode".
type DHTNode struct {
	Info   *chord.NetNode
	Port   string
	Server *rpc.Server
}

/* "Run" is called after calling "NewNode". */
func (id *DHTNode) Run() {
	listen, err := net.Listen("tcp", ":"+id.Port)
	if err != nil {
		// listen failure
		return
	}
	id.Info.Listen = listen
	id.Info.Info.Connected = true
	id.Info.Info.Init(id.Port)
	go id.Server.Accept(id.Info.Listen)
}

/* "Create" and "Join" are called after calling "Run". */
/* For a dhtNode, either "Create" or "Join" will be called, but not both. */

/* Create a new network. */
func (id *DHTNode) Create() {
	id.Info.Info.Create()
	go id.Info.Info.Stabilize()
	go id.Info.Info.FixFinger()
	go id.Info.Info.CheckPre()
}

/* Join an existing network. */
func (id *DHTNode) Join(addr string) bool {
	done := id.Info.Info.Join(addr)
	return done
}

/* Quit from the network it is currently in.*/
/* "Quit" will not be called before "Create" or "Join". */
/* For a dhtNode, "Quit" may be called for many times. */
/* For a quited node, call "Quit" again should have no effect. */
func (id *DHTNode) Quit() {
	if !id.Info.Info.Connected {
		return
	}
	id.Info.Info.Quit()
	if id.Info.Info.Connected {
		// quit failure
		fmt.Println("Quit failed")
	}
}

/* Chord offers a way of "normal" quitting. */
/* For "force quit", the node quit the network without informing other nodes. */
/* "ForceQuit" will be checked by TA manually. */
func (id *DHTNode) ForceQuit() {
	id.Info.Info.Connected = false
	err := id.Info.Listen.Close()
	if err != nil {
		// close listen failure
		fmt.Println("Force Quit failed")
		return
	}
}

/* Check whether the node represented by the IP address is in the network. */
func (id *DHTNode) Ping(addr string) bool {
	return id.Info.Info.Ping(addr)
}

/* Put a key-value pair into the network (if KEY is already in the network, cover it), or
 * get a key-value pair from the network, or
 * remove a key-value pair from the network.
 */

/* Return "true" if success, "false" otherwise. */
func (id *DHTNode) Put(key string, value string) bool {
	return id.Info.Info.Put(key, value)
}

/* Return "true" and the value if success, "false" otherwise. */
func (id *DHTNode) Get(key string) (bool, string) {
	return id.Info.Info.Get(key)
}

/* Remove the key-value pair represented by KEY from the network. */
func (id *DHTNode) Delete(key string) bool {
	return id.Info.Info.Delete(key)
}
/* Return "true" if remove successfully, "false" otherwise. */

func (id *DHTNode) Dump() {
	id.Info.Info.Dump()
}