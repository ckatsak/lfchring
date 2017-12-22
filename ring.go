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
	"fmt"
	"io"
	"io/ioutil"
	"sync/atomic"

	"golang.org/x/crypto/blake2b"
)

// Node represents a single distinct node in the ring.
type Node string

// VirtualNode represents a single virtual node in the ring.
type VirtualNode struct {
	name []byte
	node Node
	vnid uint16
}

// String returns a representation of the VirtualNode in a print-friendly
// format.
func (vn *VirtualNode) String() string {
	return fmt.Sprintf("%x (%s, %d)", vn.name, vn.node, vn.vnid)
}

// HashRing is a lock-free consistent hashing ring entity, designed for
// multiple frequent readers and infrequent updates from a single writer. It
// also supports virtual ring nodes and performs replication-related
// bookkeeping.
type HashRing struct {
	// TODO: Documentation
	state atomic.Value
}

// NewHashRing returns a new HashRing, properly initialized based on the given
// parameters, or a non-nil error value if the parameters are invalid.
//
// An arbitrary number of nodes may optionally be added to the new ring during
// initialization through parameter `nodes` (hence, NewHashRing is a variadic
// function).
func NewHashRing(replicationFactor, numVirtualNodes int, nodes ...Node) (*HashRing, error) {
	if replicationFactor < 1 || replicationFactor > (1<<8)-1 {
		return nil, fmt.Errorf("replicationFactor value %d not in (0, %d)", replicationFactor, 1<<8)
	}
	if numVirtualNodes < 1 || numVirtualNodes > (1<<16)-1 {
		return nil, fmt.Errorf("numVirtualNodes value %d not in (0, %d)", numVirtualNodes, 1<<16)
	}

	newState := &hashRingState{
		numVirtualNodes:   uint16(numVirtualNodes),
		replicationFactor: uint8(replicationFactor),
		virtualNodes:      make([]*VirtualNode, 0),
		replicaOwners:     make(map[*VirtualNode][]Node),
	}
	if len(nodes) > 0 {
		newState.add(nodes...)
	}

	ring := &HashRing{}
	ring.state.Store(newState)

	return ring, nil
}

// Size returns the number of *distinct nodes* in the ring, in its current
// state.
func (r *HashRing) Size() int {
	state := r.state.Load().(*hashRingState)
	return state.size()
}

// String returns the slice of virtual nodes of the current state of the ring,
// along with their replica owners, in a print-friendly format.
func (r *HashRing) String() string {
	state := r.state.Load().(*hashRingState)
	ret := ""
	for i, vn := range state.virtualNodes {
		ret = fmt.Sprintf("\n%s%d.  %s  -->  %v\n", ret, i, vn.String(), state.replicaOwners[vn])
	}
	return ret
}

// Add is a variadic method to add an arbitrary number of nodes in the ring
// (including all nodes' virtual nodes, of course).
//
// In the case that an already existing distinct node is attempted to be
// re-inserted to the ring, it returns an error and the ring is left untouched;
// otherwise the ring is modified as expected, and a slice (unsorted) of
// pointers to the new virtual nodes is returned.
func (r *HashRing) Add(nodes ...Node) ([]*VirtualNode, error) {
	oldState := r.state.Load().(*hashRingState)
	newState := oldState.derive()
	newVnodes, err := newState.add(nodes...)
	if err != nil {
		return nil, err
	}
	r.state.Store(newState) // <-- Atomically replace the current state
	// with the new one. At this point all new readers start working with
	// the new state. The old state will be garbage collected once the
	// existing readers (if any) are done with it.
	return newVnodes, nil
}

// Remove is a variadic method to remove an arbitrary number of nodes from the
// ring (including all nodes' virtual nodes, of course).
//
// If any of the node's virtual nodes cannot be found in the ring, a non-nil
// error value is returned and the ring is left untouched; otherwise the ring
// is modified as expected, and a slice (unsorted) of pointers to the removed
// virtual nodes is returned.
func (r *HashRing) Remove(nodes ...Node) ([]*VirtualNode, error) {
	oldState := r.state.Load().(*hashRingState)
	newState := oldState.derive()
	removedVnodes, err := newState.remove(nodes...)
	if err != nil {
		return nil, err
	}
	r.state.Store(newState) // <-- Atomically replace the current state
	// with the new one. At this point all new readers start working with
	// the new state. The old state will be garbage collected once the
	// existing readers (if any) are done with it.
	return removedVnodes, nil
}

// TODO: Documentation
func (r *HashRing) NodesForKey(key []byte) []Node {
	return r.state.Load().(*hashRingState).nodesForKey(key)
}

// TODO: Documentation
func (r *HashRing) NodesForObject(reader io.Reader) ([]Node, error) {
	objectBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	key := blake2b.Sum256(objectBytes)
	return r.NodesForKey(key[:]), nil
}

// TODO: Documentation
func (r *HashRing) VirtualNodeForKey(key []byte) *VirtualNode {
	return r.state.Load().(*hashRingState).virtualNodeForKey(key)
}

// TODO: Documentation
func (r *HashRing) Predecessor(vnodeHash []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).predecessor(vnodeHash)
}

// TODO: Documentation
func (r *HashRing) Successor(vnodeHash []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).successor(vnodeHash)
}

// TODO: Documentation
func (r *HashRing) PredecessorNode(vnodeHash []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).predecessorNode(vnodeHash)
}

// TODO: Documentation
func (r *HashRing) SuccessorNode(vnodeHash []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).successorNode(vnodeHash)
}

// TODO: Documentation
func (r *HashRing) HasVirtualNode(vnodeHash []byte) bool {
	return r.state.Load().(*hashRingState).hasVirtualNode(vnodeHash)
}

// TODO: Documentation
func (r *HashRing) IterVirtualNodes(stop <-chan struct{}) <-chan *VirtualNode {
	return r.state.Load().(*hashRingState).iterVirtualNodes(stop)
}

// TODO: Documentation
func (r *HashRing) IterReversedVirtualNodes(stop <-chan struct{}) <-chan *VirtualNode {
	return r.state.Load().(*hashRingState).iterReversedVirtualNodes(stop)
}
