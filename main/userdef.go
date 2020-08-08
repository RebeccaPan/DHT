package main

import "chord"

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

// Create a node and then return it.
func NewNode(port int) dhtNode {
	var ret DHTNode
	// Todo: init values of ret
	return &ret
}

// Todo: implement a struct which implements the interface "dhtNode".

type DHTNode struct {
	// Todo: All you need in a node which implements the interface "dhtNode".
	Data *chord.Node
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
	id.Data.Join(addr)
}

/* Quit from the network it is currently in.*/
/* "Quit" will not be called before "Create" or "Join". */
/* For a dhtNode, "Quit" may be called for many times. */
/* For a quited node, call "Quit" again should have no effect. */
func (id *DHTNode) Quit() {
	id.Quit()
}

/* Chord offers a way of "normal" quitting. */
/* For "force quit", the node quit the network without informing other nodes. */
/* "ForceQuit" will be checked by TA manually. */
func (id *DHTNode) ForceQuit() {
	id.ForceQuit()
}

/* Check whether the node represented by the IP address is in the network. */
func (id *DHTNode) Ping(addr string) bool {
	return id.Ping(addr)
}

/* Put a key-value pair into the network (if KEY is already in the network, cover it), or
 * get a key-value pair from the network, or
 * remove a key-value pair from the network.
 */

/* Return "true" if success, "false" otherwise. */
func (id *DHTNode) Put(key string, value string) bool {
	return id.Put(key, value)
}

/* Return "true" and the value if success, "false" otherwise. */
func (id *DHTNode) Get(key string) (bool, string) {
	return id.Get(key)
}

/* Remove the key-value pair represented by KEY from the network. */
func (id *DHTNode) Delete(key string) bool {
	return id.Delete(key)
}
/* Return "true" if remove successfully, "false" otherwise. */