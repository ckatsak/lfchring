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

// VirtualNodesIterator is an iterator for efficiently iterating through all
// virtual nodes in the ring in (alphanumerical) order.
type VirtualNodesIterator struct {
	ring *hashRingState
	curr int
}

// HasNext returns true if there is at least one more virtual node in the ring
// to iterate over, and false if there is none.
//
// The user of VirtualNodesIterator should always check the result of HasNext
// before calling Next to avoid panicking.
func (iter *VirtualNodesIterator) HasNext() bool {
	return iter.curr < len(iter.ring.virtualNodes)
}

// Next returns the next virtual node of the iteration.
//
// The user of VirtualNodesIterator should always check the result of HasNext
// before calling Next to avoid panicking.
func (iter *VirtualNodesIterator) Next() *VirtualNode {
	iter.curr++
	return iter.ring.virtualNodes[iter.curr-1]
}

// VirtualNodesReverseIterator is an iterator for efficiently iterating through
// all virtual nodes in the ring in reverse (alphanumerical) order.
type VirtualNodesReverseIterator struct {
	ring *hashRingState
	curr int
}

// HasNext returns true if there is at least one more virtual node in the ring
// to iterate over, and false if there is none.
//
// The user of VirtualNodesReverseIterator should always check the result of
// HasNext before calling Next to avoid panicking.
func (iter *VirtualNodesReverseIterator) HasNext() bool {
	return iter.curr >= 0
}

// Next returns the next virtual node of the (reverse) iteration.
//
// The user of VirtualNodesReverseIterator should always check the result of
// HasNext before calling Next to avoid panicking.
func (iter *VirtualNodesReverseIterator) Next() *VirtualNode {
	iter.curr--
	return iter.ring.virtualNodes[iter.curr+1]
}
