// Copyright (c) 2018, Christos Katsakioris
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//  list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package lfhashring

import (
	"bytes"
	"fmt"
	"sort"

	"golang.org/x/crypto/blake2b"
)

// TODO: Documentation
type hashRingState struct {
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
//
// derive
func (s *hashRingState) derive() *hashRingState {
	// Deep copy the slice of virtual nodes.
	newVNs := make([]*VirtualNode, len(s.virtualNodes))
	for i, vnptr := range s.virtualNodes {
		newVNs[i] = &VirtualNode{
			name: vnptr.name,
			node: vnptr.node,
			vnid: vnptr.vnid,
		}
	}
	// Initialize a new map of replica owners, **EMPTY, to be filled by
	// the caller** when needed. XXX
	newROs := make(map[*VirtualNode][]Node)

	return &hashRingState{
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

// TODO: Documentation
func (s *hashRingState) add(nodes ...Node) ([]*VirtualNode, error) {
	// Add all virtual nodes (for all distinct nodes) in ring's vnodes
	// slice, while gathering all new vnodes in a slice.
	newVnodes := make([]*VirtualNode, len(nodes)*int(s.virtualNodeCount))
	for i, node := range nodes {
		vns, err := s.addNode(node)
		if err != nil {
			return nil, err
		}
		for j, vn := range vns {
			newVnodes[i*len(vns)+j] = vn
		}
	}
	// Sort state's vnodes slice.
	sort.Slice(s.virtualNodes, func(i, j int) bool {
		if bytes.Compare(s.virtualNodes[i].name, s.virtualNodes[j].name) < 0 {
			return true
		}
		return false
	})
	s.fixReplicaOwners()

	// Return the slice of new vnodes, sorted. // FIXME: unsorted actually: sorted per node; nodes appended.
	return newVnodes, nil
}

// TODO: Documentation
func (s *hashRingState) addNode(node Node) ([]*VirtualNode, error) {
	newVnodes := make([]*VirtualNode, s.virtualNodeCount)
	for vnid := uint16(0); vnid < s.virtualNodeCount; vnid++ {
		newVnodes[vnid] = s.addVirtualNode(node, vnid)
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

// TODO: Documentation
func (s *hashRingState) addVirtualNode(node Node, vnid uint16) *VirtualNode {
	// Create a new virtual node for Node `node` and append it to the slice
	// of new vnodes.
	newVnodeDigest := blake2b.Sum256([]byte(fmt.Sprintf("%s-%d", node, vnid)))
	newVnode := &VirtualNode{
		name: newVnodeDigest[:],
		node: node,
		vnid: vnid,
	}
	return newVnode
}

// TODO: Documentation
func (s *hashRingState) remove(nodes ...Node) ([]*VirtualNode, error) {
	// Remove all virtual nodes (of all distinct nodes) from state's vnodes
	// slice, isolating them in a new slice.
	removedVnodes := make([]*VirtualNode, s.virtualNodeCount)
	for i, node := range nodes {
		vns, err := s.removeNode(node)
		if err != nil {
			return nil, err
		}
		for j, vn := range vns {
			removedVnodes[i*len(vns)+j] = vn
		}
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
func (s *hashRingState) removeNode(node Node) ([]*VirtualNode, error) {
	removedIndices := make([]int, s.virtualNodeCount)
	for vnid := uint16(0); vnid < s.virtualNodeCount; vnid++ {
		if removedIndex, err := s.removeVirtualNode(node, vnid); err != nil {
			return nil, err
		} else {
			removedIndices[vnid] = removedIndex
		}
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

// TODO: Documentation
func (s *hashRingState) removeVirtualNode(node Node, vnid uint16) (int, error) {
	digest := blake2b.Sum256([]byte(fmt.Sprintf("%s-%d", node, vnid)))
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
		for _, vn := range s.virtualNodes {
			select {
			case <-stop:
				return
			case retChan <- vn:
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
