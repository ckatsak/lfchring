// Copyright 2018 Christos Katsakioris
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lfchring

import (
	"bytes"
	"fmt"
	"sort"
)

// hashRingState represents a state of the HashRing, and this is why it is not
// exported; it may only be manipulated by the clients via its wrapper type,
// the HashRing.
//
// The hashRingState of a HashRing is meant to be updated by one single writer
// using a RCU-like technique, and this is what makes this whole implementation
// of the consistent hashing ring data structure to be lock-free.
type hashRingState struct {
	// hash is the hash function used for all supported consistent hashing
	// ring functionality and operations.
	hash func([]byte) []byte

	// virtualNodeCount is the number of virtual nodes that each of the
	// distinct nodes in the ring has.
	//
	// It is set during ring's initialization and should not be modified
	// later.
	virtualNodeCount uint16

	// replicationFactor is the number of distinct nodes in the ring that
	// own each of the keys.
	//
	// It is set during ring's initialization and should not be modified
	// later.
	replicationFactor uint8

	// virtualNodes is a sorted slice of pointers to VirtualNode structs,
	// which correspond to each of the virtual nodes of all distinct nodes
	// that are members of the ring in its current state.
	virtualNodes []*VirtualNode

	// replicaOwners maps each virtual node to a set of distinct nodes that
	// are members of the ring in its current state, and which should own
	// replicas of that virtual node's keys in this state.
	replicaOwners map[*VirtualNode][]Node
}

// TODO: Documentation
func (s *hashRingState) derive() *hashRingState {
	// Deep copy the slice of virtual nodes.
	newVNs := make([]*VirtualNode, len(s.virtualNodes))
	for i := range s.virtualNodes {
		newVNs[i] = &VirtualNode{
			name: s.virtualNodes[i].name,
			node: s.virtualNodes[i].node,
			vnid: s.virtualNodes[i].vnid,
		}
	}
	// Initialize a new map of replica owners, **EMPTY, to be filled by
	// the caller** when needed. XXX
	newROs := make(map[*VirtualNode][]Node)

	return &hashRingState{
		hash:              s.hash,
		replicationFactor: s.replicationFactor,
		virtualNodeCount:  s.virtualNodeCount,
		virtualNodes:      newVNs,
		replicaOwners:     newROs,
	}
}

// TODO: Documentation
func (s *hashRingState) size() int {
	return len(s.virtualNodes) / int(s.virtualNodeCount)
}

// insert is a variadic method to insert an arbitrary number of nodes in the
// hashRingState (including all nodes' virtual nodes, of course).
//
// In the case that an already existing distinct node is attempted to be
// re-inserted, insert returns a non-nil error value and the state is left
// untouched. Otherwise, the state is modified as expected, and a slice
// (unsorted) of pointers to the new virtual nodes is returned.
func (s *hashRingState) insert(nodes ...Node) ([]*VirtualNode, error) {
	// Add all virtual nodes (for all distinct nodes) in ring's vnodes
	// slice, while gathering all new vnodes in a slice.
	newVnodes := make([]*VirtualNode, len(nodes)*int(s.virtualNodeCount))
	for i := range nodes {
		vns, err := s.insertNode(nodes[i])
		if err != nil {
			return nil, err
		}
		copy(newVnodes[i*len(vns):(i+1)*len(vns)], vns)
	}
	// Sort state's vnodes slice.
	sort.Slice(s.virtualNodes, func(i, j int) bool {
		if bytes.Compare(s.virtualNodes[i].name, s.virtualNodes[j].name) < 0 {
			return true
		}
		return false
	})
	s.fixReplicaOwners()

	// Return the slice of new vnodes, unsorted.
	return newVnodes, nil
}

// insertNode inserts all virtual nodes of a distinct ring node `node` in the
// state's slice of virtual nodes, and returns a sorted slice of them, or an
// error if the node seems to be already in.
//
// To decide whether the node is already in the ring or not, it is checked
// whether one of the new virtual nodes (vnid 0, hence random order) is already
// in or not, before appending all of them to the state's slice of virtual
// nodes.
//
// In the extremely unlikely case of a conflict, insertNode has low chances of
// uncovering it, especially as virtualNodeCount or the size of the ring get
// bigger.
func (s *hashRingState) insertNode(node Node) ([]*VirtualNode, error) {
	newVnodes := make([]*VirtualNode, s.virtualNodeCount)
	for vnid := uint16(0); vnid < s.virtualNodeCount; vnid++ {
		newVnodes[vnid] = s.insertVirtualNode(node, vnid)
	}

	// Check whether the distinct node is already in the ring, by checking
	// if the first virtual node in the slice of the new ones (which lie in
	// random order) is already in state's vnodes slice.
	i := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, newVnodes[0].name) == -1 {
			return false
		}
		return true
	})
	if i < len(s.virtualNodes) && bytes.Compare(s.virtualNodes[i].name, newVnodes[0].name) == 0 {
		return nil, fmt.Errorf("virtual node {%s} is already in the ring", newVnodes[0])
	}

	// Append the new vnodes to state's slice of vnodes.
	s.virtualNodes = append(s.virtualNodes, newVnodes...)
	return newVnodes, nil
}

// insertVirtualNode returns a ready *VirtualNode for node's virtual node with the
// given vnid.
func (s *hashRingState) insertVirtualNode(node Node, vnid uint16) *VirtualNode {
	// Create a new virtual node for Node `node` and append it to the slice
	// of new vnodes.
	newVnodeDigest := s.hash([]byte(fmt.Sprintf("%s-%d", node, vnid)))
	newVnode := &VirtualNode{
		name: newVnodeDigest[:],
		node: node,
		vnid: vnid,
	}
	return newVnode
}

// remove is a variadic method to remove an arbitrary number of nodes from the
// hashRingState (including all nodes' virtual nodes, of course).
//
// If any of the nodes' virtual nodes cannot be found in the ring, a non-nil
// error value is returned and the state is left untouched. Otherwise the state
// is modified as expected, and a slice (unsorted) of pointers to the removed
// virtual nodes is returned.
func (s *hashRingState) remove(nodes ...Node) ([]*VirtualNode, error) {
	// Remove all virtual nodes (of all distinct nodes) from state's vnodes
	// slice, isolating them in a new slice.
	removedVnodes := make([]*VirtualNode, len(nodes)*int(s.virtualNodeCount))
	for i := range nodes {
		vns, err := s.removeNode(nodes[i])
		if err != nil {
			return nil, err
		}
		copy(removedVnodes[i*len(vns):(i+1)*len(vns)], vns)
	}
	// Sort state's vnodes slice.
	sort.Slice(s.virtualNodes, func(i, j int) bool {
		if bytes.Compare(s.virtualNodes[i].name, s.virtualNodes[j].name) < 0 {
			return true
		}
		return false
	})
	s.fixReplicaOwners()

	// Return the slice of the removed vnodes (unsorted).
	return removedVnodes, nil
}

// TODO: Documentation
//
// removeNode removes all virtual nodes of the given distinct ring node from
// the state, and returns a sorted slice of them.
//
// First, it figures out what are the indices of the virtual nodes that should
// be removed (by calling the removeVirtualNode method for each one of them).
// Then, it builds a new slice of virtual nodes for the state, excluding the
// aforementioned indices.
//
// Complexity: O( (V*N)*log(V*N) )
func (s *hashRingState) removeNode(node Node) ([]*VirtualNode, error) {
	removedIndices := make([]int, s.virtualNodeCount)
	for vnid := uint16(0); vnid < s.virtualNodeCount; vnid++ {
		removedIndex, err := s.removeVirtualNode(node, vnid)
		if err != nil {
			return nil, err
		}
		removedIndices[vnid] = removedIndex
	}
	sort.Ints(removedIndices)

	removedVnodes := make([]*VirtualNode, s.virtualNodeCount)
	newRingVirtualNodes := make([]*VirtualNode, len(s.virtualNodes)-int(s.virtualNodeCount))
	rii, nvni, ovni := 0, 0, 0
	for ; nvni < len(newRingVirtualNodes) && rii < len(removedIndices); ovni++ {
		if ovni == removedIndices[rii] {
			removedVnodes[rii] = s.virtualNodes[ovni]
			rii++
		} else {
			newRingVirtualNodes[nvni] = s.virtualNodes[ovni]
			nvni++
		}
	}
	if nvni == len(newRingVirtualNodes) {
		for ; rii < len(removedIndices); rii++ {
			removedVnodes[rii] = s.virtualNodes[removedIndices[rii]]
		}
	}
	if rii == len(removedIndices) {
		for ; nvni < len(newRingVirtualNodes); nvni, ovni = nvni+1, ovni+1 {
			newRingVirtualNodes[nvni] = s.virtualNodes[ovni]
		}
	}
	s.virtualNodes = newRingVirtualNodes
	return removedVnodes, nil
}

// removeVirtualNode returns the index of state's slice of virtual nodes which
// refers to the virtual node that is specified by the given node and vnid, or
// an error if the virtual node does not exist.
func (s *hashRingState) removeVirtualNode(node Node, vnid uint16) (int, error) {
	digest := s.hash([]byte(fmt.Sprintf("%s-%d", node, vnid)))
	i := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, digest[:]) == -1 {
			return false
		}
		return true
	})
	if bytes.Compare(s.virtualNodes[i].name, digest[:]) != 0 {
		return -1, fmt.Errorf("virtual node {%x (%s, %d)} is not in the ring", digest, node, vnid)
	}
	return i, nil
}

// fixReplicaOwners creates state's replicaOwners (the map of virtual nodes to
// replica-owner distinct ring nodes) anew, to re-adjust it after the addition
// or the removal of one or more distinct ring nodes.
func (s *hashRingState) fixReplicaOwners() {
	for i := 0; i < len(s.virtualNodes); i++ {
		vnode := s.virtualNodes[i] // auxiliary
		s.replicaOwners[vnode] = make([]Node, s.replicationFactor)
		s.replicaOwners[vnode][0] = s.virtualNodes[i].node

		j := i                       // j: index i --> len(s.virtualNodes) --> 0 --> i-1
		k := s.replicationFactor - 1 // k: # of subsequent nodes remaining to be found
		for k > 0 {
			// Get j, the next index in state's vnodes slice.
			j = (j + 1) % len(s.virtualNodes)
			// If cycle, break. even if k > 0; it means that s.replicationFactor > # of nodes.
			if j == i {
				break
			}
			currNode := s.virtualNodes[j].node // the node we are on for this `i`'s (index `j`-)traversal
			nodePresent := false               // flag to raise if currNode is already in s.replicaOwners
			// As we want distinct nodes only in s.replicaOwners, make sure currNode is not already in.
			for _, l := range s.replicaOwners[vnode] {
				if currNode == l {
					nodePresent = true
					break
				}
			}
			// If currNode is not already in, get it in, and decrease # of nodes remaining to be found.
			if !nodePresent {
				s.replicaOwners[vnode][s.replicationFactor-k] = currNode
				k--
			}
		}
		// If cycled above, set slice's length so as to address the useful values only:
		if j == i {
			s.replicaOwners[vnode] = s.replicaOwners[vnode][:s.replicationFactor-k]
			// NOTE: There is a memory leak: the amount of memory that is allocated for the
			// slice of every key in s.replicaOwners is more than the required amount when
			// such cycles happen (i.e. when replicationFactor > number of distinct nodes).
			// To fix this, allocate a temp slice as make([]Node, 1, s.replicationFactor)
			// and immediately fill it with the distinct Node that the vnode belongs to,
			// then append any extra Nodes found, and in the end, allocate a new slice of
			// capacity equal to temp slice's *length*, and copy(newSlice, temp).
			// This, however, would result in lower performance (more allocations) in my
			// own average use cases (that replicationFactor <= number of distinct nodes),
			// so I'm not interested in changing it for now.
		}
	}
}

// TODO: Documentation
func (s *hashRingState) virtualNodeForKey(key []byte) *VirtualNode {
	index := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, key) == -1 {
			return false
		}
		return true
	})
	if index == len(s.virtualNodes) {
		index = 0
	}
	return s.virtualNodes[index]
}

// TODO: Documentation
func (s *hashRingState) nodesForKey(key []byte) []Node {
	return s.replicaOwners[s.virtualNodeForKey(key)]
}

// TODO: Documentation
func (s *hashRingState) predecessor(vnodeHash []byte) (*VirtualNode, error) {
	if len(s.virtualNodes) == 0 {
		return nil, fmt.Errorf("empty ring")
	}
	index := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, vnodeHash) == -1 {
			return false
		}
		return true
	})
	if index == 0 {
		index = len(s.virtualNodes)
	}
	index--
	return s.virtualNodes[index], nil
}

// TODO: Documentation
func (s *hashRingState) successor(vnodeHash []byte) (*VirtualNode, error) {
	if len(s.virtualNodes) == 0 {
		return nil, fmt.Errorf("empty ring")
	}
	index := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, vnodeHash) == -1 {
			return false
		}
		return true
	})
	index = (index + 1) % len(s.virtualNodes)

	return s.virtualNodes[index], nil
}

// TODO: Documentation
func (s *hashRingState) predecessorNode(vnodeHash []byte) (*VirtualNode, error) {
	switch s.size() {
	case 0:
		return nil, fmt.Errorf("empty ring")
	case 1:
		return nil, fmt.Errorf("single-distinct-node ring")
	default:
	}

	index := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, vnodeHash) == -1 {
			return false
		}
		return true
	})

	var currNode Node
	switch index {
	case 0:
		index = len(s.virtualNodes)
		fallthrough
	case len(s.virtualNodes):
		// This case should be unreachable as long as vnodeHash is
		// actually a virtual node in the current state of the ring.
		// Execution flow may reach this point only by falling-through
		// in the case that index == 0.
		currNode = s.virtualNodes[0].node
	default:
		currNode = s.virtualNodes[index].node
	}
	index--

	for {
		if s.virtualNodes[index].node != currNode {
			return s.virtualNodes[index], nil
		}
		index--
		if index < 0 {
			index = len(s.virtualNodes) - 1
		}
	}
}

// TODO: Documentation
func (s *hashRingState) successorNode(vnodeHash []byte) (*VirtualNode, error) {
	switch s.size() {
	case 0:
		return nil, fmt.Errorf("empty ring")
	case 1:
		return nil, fmt.Errorf("single-distinct-node ring")
	default:
	}

	index := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, vnodeHash) == -1 {
			return false
		}
		return true
	})

	if index == len(s.virtualNodes) {
		// NOTE: This case should be unreachable as long as vnodeHash
		// is actually a virtual node in the current state of the ring.
		index = 0
	}
	currNode := s.virtualNodes[index].node
	index = (index + 1) % len(s.virtualNodes)

	for ; ; index = (index + 1) % len(s.virtualNodes) {
		if s.virtualNodes[index].node != currNode {
			return s.virtualNodes[index], nil
		}
	}
}

// TODO: Documentation
func (s *hashRingState) hasVirtualNode(vnodeHash []byte) bool {
	index := sort.Search(len(s.virtualNodes), func(j int) bool {
		if bytes.Compare(s.virtualNodes[j].name, vnodeHash) == -1 {
			return false
		}
		return true
	})
	return index != len(s.virtualNodes) && bytes.Compare(s.virtualNodes[index].name, vnodeHash) == 0
}

// TODO: Documentation
func (s *hashRingState) iterVirtualNodes(stop <-chan struct{}) <-chan *VirtualNode {
	retChan := make(chan *VirtualNode)
	go func() {
		defer close(retChan)
		for i := range s.virtualNodes {
			select {
			case <-stop:
				return
			case retChan <- s.virtualNodes[i]:
			}
		}
	}()
	return retChan
}

// TODO: Documentation
func (s *hashRingState) iterReversedVirtualNodes(stop <-chan struct{}) <-chan *VirtualNode {
	retChan := make(chan *VirtualNode)
	go func() {
		defer close(retChan)
		for i := len(s.virtualNodes) - 1; i >= 0; i-- {
			select {
			case <-stop:
				return
			case retChan <- s.virtualNodes[i]:
			}
		}
	}()
	return retChan
}
