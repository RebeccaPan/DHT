package main

import (
	chord "chord"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	// Create a node and then return it.
	var node DHTNode
	// Todo: assign init val to node
	return &node
}

// Implement a struct which implements the interface "dhtNode".
type DHTNode struct {
	// Todo: All you need in a node which implements the interface "dhtNode".
	Info    *chord.NetNode
	Port string

}

/* "Run" is called after calling "NewNode". */
func (id *DHTNode) Run() {
	// Todo: Run()
}

/* "Create" and "Join" are called after calling "Run". */
/* For a dhtNode, either "Create" or "Join" will be called, but not both. */

/* Create a new network. */
func (id *DHTNode) Create() {
	// Todo: Create()
}

/* Join an existing network. */
func (id *DHTNode) Join(addr string) bool {
	done := id.Info.Info.Join(addr)
	// Todo: Join()
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
		// quit error
	}
}

/* Chord offers a way of "normal" quitting. */
/* For "force quit", the node quit the network without informing other nodes. */
/* "ForceQuit" will be checked by TA manually. */
func (id *DHTNode) ForceQuit() {
	// Todo: ForceQuit()
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