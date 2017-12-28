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
	return fmt.Sprintf("%x (%q, %d)", vn.name, vn.node, vn.vnid)
}

// HashRing is a lock-free consistent hashing ring entity, designed for
// multiple frequent readers and infrequent updates from a single writer. It
// also supports virtual ring nodes and performs replication-related
// bookkeeping.
type HashRing struct {
	// state is an atomic.Value meant to hold values of type
	// *hashRingState. Its use is what makes this implementation of the
	// consistent hashing ring concurrent data structure lock-free. Note
	// however that this only works for a single writer. For multiple
	// writers, an additional mutex among them would be needed.
	state atomic.Value
}

// NewHashRing returns a new HashRing, properly initialized based on the given
// parameters, or a non-nil error value if the parameters are invalid.
//
// An arbitrary number of nodes may optionally be added to the new ring during
// initialization through parameter `nodes` (hence, NewHashRing is a variadic
// function).
func NewHashRing(replicationFactor, virtualNodeCount int, nodes ...Node) (*HashRing, error) {
	if replicationFactor < 1 || replicationFactor > (1<<8)-1 {
		return nil, fmt.Errorf("replicationFactor value %d not in (0, %d)", replicationFactor, 1<<8)
	}
	if virtualNodeCount < 1 || virtualNodeCount > (1<<16)-1 {
		return nil, fmt.Errorf("virtualNodeCount value %d not in (0, %d)", virtualNodeCount, 1<<16)
	}

	newState := &hashRingState{
		virtualNodeCount:  uint16(virtualNodeCount),
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

// Clone returns a new ring which is a deep copy of the original one.
func (r *HashRing) Clone() *HashRing {
	newState := r.state.Load().(*hashRingState).derive()
	newState.fixReplicaOwners()
	newRing := &HashRing{}
	newRing.state.Store(newState)
	return newRing
}

// Size returns the number of *distinct nodes* in the ring, in its current
// state.
func (r *HashRing) Size() int {
	return r.state.Load().(*hashRingState).size()
}

// String returns the slice of virtual nodes of the current state of the ring,
// along with their replica owners, as a "print-friendly" string.
func (r *HashRing) String() string {
	state := r.state.Load().(*hashRingState)
	ret := bytes.Buffer{}
	for i, vn := range state.virtualNodes {
		if _, err := ret.WriteString(fmt.Sprintf("%d.  %s  =>  %q\n", i, vn, state.replicaOwners[vn])); err != nil {
			return "Ring too large to be represented in a string."
		}
	}
	return ret.String()
}

// Add is a variadic method to add an arbitrary number of nodes in the ring
// (including all nodes' virtual nodes, of course).
//
// In the case that an already existing distinct node is attempted to be
// re-inserted to the ring, Add returns a non-nil error value and the ring is
// left untouched. Otherwise, the ring is modified as expected, and a slice
// (unsorted) of pointers to the new virtual nodes is returned.
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
// If any of the nodes' virtual nodes cannot be found in the ring, a non-nil
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

// NodesForKey returns a slice of Nodes (of length equal to the configured
// replication factor) that are currently responsible for holding the given
// key.
//
// Complexity: O( log(V*N) )
func (r *HashRing) NodesForKey(key []byte) []Node {
	return r.state.Load().(*hashRingState).nodesForKey(key)
}

// NodesForObject returns a slice of Nodes (of length equal to the configured
// replication factor) that are currently responsible for holding the object
// that can be read from the given io.Reader (blake2b hashing is applied
// first). It returns a non-nil error value in the case of a failure while
// reading from the io.Reader.
//
// Complexity: O( Read ) + O( blake2b ) + O( log(V*N) )
func (r *HashRing) NodesForObject(reader io.Reader) ([]Node, error) {
	objectBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	key := blake2b.Sum256(objectBytes)
	return r.NodesForKey(key[:]), nil
}

// VirtualNodeForKey returns the virtual node in the ring that the given key
// would be assigned to.
//
// Complexity: O( log(V*N) )
func (r *HashRing) VirtualNodeForKey(key []byte) *VirtualNode {
	return r.state.Load().(*hashRingState).virtualNodeForKey(key)
}

// Predecessor returns the virtual node which is predecessor to the one that
// the given key would be assigned to. It returns a non-nil error if the ring
// is empty.
//
// Complexity: O( log(V*N) )
func (r *HashRing) Predecessor(key []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).predecessor(key)
}

// Successor returns the virtual node which is successor to the one that the
// given key would be assigned to. It returns a non-nil error if the ring is
// empty.
//
// Complexity: O( log(V*N) )
func (r *HashRing) Successor(key []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).successor(key)
}

// PredecessorNode returns the virtual node which is the first predecessor to
// the one that the given key would be assigned to, but also belongs to a
// different node than the latter. It returns a non-nil error if the ring
// either is empty or consists of a single distinct node.
//
// Complexity: O( log(V*N)+V )
func (r *HashRing) PredecessorNode(key []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).predecessorNode(key)
}

// SuccessorNode returns the virtual node which is the first successor to the
// one that the given key would be assigned to, but also belongs to a different
// node than the latter. It returns a non-nil error if the ring either is empty
// or consists of a single distinct node.
//
// Complexity: O( log(V*N)+V )
func (r *HashRing) SuccessorNode(key []byte) (*VirtualNode, error) {
	return r.state.Load().(*hashRingState).successorNode(key)
}

// HasVirtualNode returns true if the given key corresponds to a virtual node
// in the ring, or false otherwise.
//
// Complexity: O( log(V*N) )
func (r *HashRing) HasVirtualNode(key []byte) bool {
	return r.state.Load().(*hashRingState).hasVirtualNode(key)
}

// VirtualNodes allows memory-efficient iteration over all virtual nodes in the
// ring, by returning a channel for the caller to read the virtual nodes from.
// The stop channel parameter should be used to avoid memory leaks when
// quitting the iteration early.
func (r *HashRing) VirtualNodes(stop <-chan struct{}) <-chan *VirtualNode {
	return r.state.Load().(*hashRingState).iterVirtualNodes(stop)
}

// VirtualNodesReversed allows memory-efficient iteration over all virtual
// nodes in the ring in reverse order, by returning a channel for the caller to
// read the virtual nodes from. The stop channel parameter should be used to
// avoid memory leaks when quitting the iteration early.
func (r *HashRing) VirtualNodesReversed(stop <-chan struct{}) <-chan *VirtualNode {
	return r.state.Load().(*hashRingState).iterReversedVirtualNodes(stop)
}
