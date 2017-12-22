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
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/blake2b"
)

func checkVirtualNodes(t *testing.T, r *HashRing) {
	t.Helper()
	state := r.state.Load().(*hashRingState)
	numVNIDs := make(map[Node]int)
	for i := 0; i < len(state.virtualNodes)-1; i++ {
		if _, exists := numVNIDs[state.virtualNodes[i].node]; exists {
			numVNIDs[state.virtualNodes[i].node]++
		} else {
			numVNIDs[state.virtualNodes[i].node] = 1
		}
		if bytes.Compare(state.virtualNodes[i].name, state.virtualNodes[i+1].name) >= 0 {
			t.Errorf("%x == state.virtualNodes[%d] >= state.virtualNodes[%d] == %x\n",
				state.virtualNodes[i], i, i+1, state.virtualNodes[i+1])
		}
	}
	// Include the last one, which was left out of the loop above.
	if _, exists := numVNIDs[state.virtualNodes[len(state.virtualNodes)-1].node]; exists {
		numVNIDs[state.virtualNodes[len(state.virtualNodes)-1].node]++
	} else {
		numVNIDs[state.virtualNodes[len(state.virtualNodes)-1].node] = 1
	}
	// Check if all nodes were counted.
	if len(numVNIDs) != r.Size() {
		t.Errorf("Counted %d nodes; expected %d.\n", len(numVNIDs), r.Size())
	}
	// Check if all vnodes were counted.
	numVnodes := 0
	for _, vnCount := range numVNIDs {
		numVnodes += vnCount
	}
	if numVnodes != r.Size()*int(state.numVirtualNodes) {
		t.Errorf("Counted %d virtual nodes; expected %d.\n", numVnodes, r.Size()*int(state.numVirtualNodes))
	}
	t.Log("Ring state's slice of virtual nodes looks OK.")
}

func TestNewRingBadValues(t *testing.T) {
	if _, err := NewHashRing(1<<8, 1<<16); err != nil {
		t.Logf("NewHashRing(1<<8, 1<<16): %v\n", err)
	} else {
		t.Errorf("Expected error from NewHashRing()\n")
	}
	if _, err := NewHashRing(1<<8-1, 1<<16); err != nil {
		t.Logf("NewHashRing(1<<8-1, 1<<16): %v\n", err)
	} else {
		t.Errorf("Expected error from NewHashRing()\n")
	}
}

func TestNewEmptyRing(t *testing.T) {
	r, err := NewHashRing(3, 4)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	if r.Size() != 0 {
		t.Errorf("Size of empty ring was reported to be %d.\n", r.Size())
	}
}

func TestNewRingReplicationFactorLessThanVirtualNodeCount(t *testing.T) {
	r, err := NewHashRing(3, 2, "node-0", "node-1")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	checkVirtualNodes(t, r)
	t.Log(r)
}

func testNewRing(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	if r.Size() != numNodes {
		t.Errorf("r.Size() == %d; expected %d.\n", r.Size(), numNodes)
	}
}
func TestNewTinyRing(t *testing.T)     { testNewRing(t, 3, 4, 4) }
func TestNewMedium1Ring(t *testing.T)  { testNewRing(t, 4, 64, 32) }
func TestNewMedium2Ring(t *testing.T)  { testNewRing(t, 4, 128, 8) }
func TestNewBigRing(t *testing.T)      { testNewRing(t, 4, 128, 128) }
func TestNewHugeRing(t *testing.T)     { testNewRing(t, 4, 256, 512) }
func TestNewGiganticRing(t *testing.T) { testNewRing(t, 4, 512, 2048) }

func TestAddExistingNode(t *testing.T) {
	r, err := NewHashRing(2, 4, "node-0", "node-1")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	_, err = r.Add("node-0")
	if err != nil {
		t.Logf("Add(): returned error %q, as expected\n", err.Error())
	} else {
		t.Errorf("Add(): Expected an error for adding an existing node\n", err)
	}
}

func testAdd(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	// Create the ring
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	for i := numNodes; i < numNodes+10; i += 2 {
		_, err := r.Add(Node(fmt.Sprintf("node-%d", i)), Node(fmt.Sprintf("node-%d", i+1)))
		if err != nil {
			t.Errorf("Add(): %v\n", err)
		}
	}
	checkVirtualNodes(t, r)
}
func TestAddTinyRing(t *testing.T)     { testAdd(t, 3, 4, 3) }
func TestAddMedium1Ring(t *testing.T)  { testAdd(t, 3, 64, 32) }
func TestAddMedium2Ring(t *testing.T)  { testAdd(t, 3, 128, 8) }
func TestAddBigRing(t *testing.T)      { testAdd(t, 3, 128, 128) }
func TestAddHugeRing(t *testing.T)     { testAdd(t, 2, 256, 512) }
func TestAddGiganticRing(t *testing.T) { testAdd(t, 2, 512, 1024) }

func testParallelRW(t *testing.T, replicationFactor, numVnodes, numNodes, concurrency int) {
	// Create the ring
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	_, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	// TODO
}

// NOTE: The ring is not expected to be able to handle multiple writers.
func testParallelAdd(t *testing.T, replicationFactor, numVnodes, numNodes, concurrency int) {
	// Create the ring
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	c := make(chan int, concurrency)
	done := make(chan struct{})
	go func() {
		defer close(c)
		for i := numNodes; i < numNodes+concurrency; i++ {
			c <- i
		}
	}()

	runtime.GOMAXPROCS(runtime.NumCPU())
	start := time.Now()
	for goroutine := 0; goroutine < concurrency; goroutine++ {
		go func(workerID int) {
			for i := range c {
				n := Node(fmt.Sprintf("node-%d", i))
				t.Logf("[goroutine-%d] +%s: Adding \"%s\"\n", workerID, time.Since(start), n)
				_, err := r.Add(n)
				if err != nil {
					t.Errorf("[goroutine-%d] +%s: r.Add(): %v\n", workerID, time.Since(start), err)
				}
			}
			done <- struct{}{}
		}(goroutine)
	}

	t.Logf("[main-goroutine] +%s: Waiting for all goroutines to finish...", time.Since(start))
	for i := 0; i < concurrency; i++ {
		<-done
	}

	if r.Size() != numNodes+concurrency {
		t.Errorf("[main-goroutine] +%s: r.Size() == %d; expected %d.\n",
			time.Since(start), r.Size(), numNodes+concurrency)
	} else {
		t.Logf("[main-goroutine] +%s: The number of total nodes looks OK.", time.Since(start))
	}
}

//func TestParallelAddTinyRing(t *testing.T)    { testParallelAdd(t, 3, 2, 2, 3) }
//func TestParallelAddMedium2Ring(t *testing.T) { testParallelAdd(t, 3, 128, 2, 10) }

func TestRemoveNontExistentNode(t *testing.T) {
	r, err := NewHashRing(2, 8, "node-0", "node-1", "node-2")
	if err != nil {
		t.Errorf("NewHashRing(): %v", err)
		t.FailNow()
	}
	_, err = r.Remove("node-42")
	if err != nil {
		t.Logf("Remove(): returned error %q, as expected\n", err.Error())
	} else {
		t.Errorf("Remove(): expected error for removing non-existent node\n")
	}
	checkVirtualNodes(t, r)
}

func testRemoveFromRing(t *testing.T, replicationFactor, numVirtualNodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVirtualNodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	checkVirtualNodes(t, r)

	if r.Size() != numNodes {
		t.Errorf("r.Size() == %d; expected %d.\n", r.Size(), numNodes)
	} else {
		t.Log("r.Size() before removal:", r.Size())
	}

	for i := 0; i < r.Size(); i += 2 {
		if _, err := r.Remove(Node(fmt.Sprintf("node-%d", i))); err != nil {
			t.Errorf("(*HashRing).Remove(\"node-%d\"): %v\n", i, err)
			t.FailNow()
		} else {
			numNodes--
		}
	}
	checkVirtualNodes(t, r)
	if r.Size() != numNodes {
		t.Errorf("r.Size() == %d; expected %d.\n", r.Size(), numNodes)
	} else {
		t.Log("r.Size() after removal:", r.Size(), "OK")
	}
}
func TestRemoveFromTinyRing(t *testing.T)    { testRemoveFromRing(t, 3, 2, 4) }
func TestRemoveFromMedium1Ring(t *testing.T) { testRemoveFromRing(t, 3, 64, 32) }
func TestRemoveFromMedium2Ring(t *testing.T) { testRemoveFromRing(t, 3, 128, 8) }
func TestRemoveFromBigRing(t *testing.T)     { testRemoveFromRing(t, 3, 128, 128) }
func TestRemoveFromHugeRing(t *testing.T)    { testRemoveFromRing(t, 3, 256, 512) }

func TestNodesForKeyTinyRing(t *testing.T) {
	numNodes := 5
	replicationFactor := 4
	numVnodes := 4

	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	joinedResults := make([]string, 0)
	for _, vn := range r.state.Load().(*hashRingState).virtualNodes {
		joinedResults = append(joinedResults, "\n"+vn.String()+" (vnode)\n")
	}

	for i := 0x00; i < 0x10; i++ {
		// keyS examples: 000...00, 111...11, ..., fff...ff
		//                <- 64 ->  <- 64 ->       <- 64 ->
		keyS := strings.Repeat(strconv.FormatInt(int64(i), 16), 64)
		// keyB is the 64B-long keyS string's []byte representation ([32]byte underneath)
		keyB, _ := hex.DecodeString(keyS)
		nodes := r.NodesForKey(keyB)
		joinedResults = append(joinedResults, fmt.Sprintf("\n%s (key)\n\t@ nodes: %v\n", keyS, nodes))
	}

	sort.Strings(joinedResults)
	t.Log(joinedResults)
}

func TestNodesForObjectBadReader(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pw.CloseWithError(fmt.Errorf("test"))
	defer pr.Close()

	r, err := NewHashRing(3, 3)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	if _, err = r.NodesForObject(pr); err != nil {
		t.Logf("NodesForObject(): Returned error %q, as expected\n", err)
	} else {
		t.Errorf("NodesForObject(): expected an error")
	}
}

func TestNodesForObjectTinyRing(t *testing.T) {
	numNodes := 5
	replicationFactor := 4
	numVnodes := 4

	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	joinedResults := make([]string, 0)
	for _, vn := range r.state.Load().(*hashRingState).virtualNodes {
		joinedResults = append(joinedResults, "\n"+vn.String()+" (vnode)\n")
	}

	for i := 0x00; i < 0x10; i++ {
		objB := []byte(strconv.FormatInt(int64(i), 16))
		keyB := blake2b.Sum256(objB)
		keyS := hex.EncodeToString(keyB[:])

		nodes, err := r.NodesForObject(bytes.NewReader(objB))
		if err != nil {
			t.Error(err)
		}
		joinedResults = append(joinedResults, fmt.Sprintf("\n%s (key)\n\t@nodes: %v\n", keyS, nodes))
	}

	sort.Strings(joinedResults)
	t.Log(joinedResults)
}

func testIter(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	iterVNList := make([]*VirtualNode, 0)
	for vn := range r.IterVirtualNodes() {
		iterVNList = append(iterVNList, vn)
	}

	if len(iterVNList) != numVnodes*numNodes {
		t.Errorf("len(iterVNList) == %d != %d == numVnodes*numNodes\n", len(iterVNList), numVnodes*numNodes)
		t.FailNow()
	} else {
		t.Logf("The number of iterated virtual nodes looks OK.")
	}
}
func TestIterTinyRing(t *testing.T)     { testIter(t, 3, 4, 4) }
func TestIterMedium1Ring(t *testing.T)  { testIter(t, 2, 64, 32) }
func TestIterMedium2Ring(t *testing.T)  { testIter(t, 2, 128, 2) }
func TestIterBigRing(t *testing.T)      { testIter(t, 2, 128, 128) }
func TestIterHugeRing(t *testing.T)     { testIter(t, 2, 256, 512) }
func TestIterGiganticRing(t *testing.T) { testIter(t, 2, 512, 1024) }

func testParallelIter(t *testing.T, replicationFactor, numVnodes, numNodes, concurrency int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	done := make(chan struct{})
	runtime.GOMAXPROCS(runtime.NumCPU())
	start := time.Now()
	for goroutine := 0; goroutine < concurrency; goroutine++ {
		go func(workerID int) {
			iterList := make([]*VirtualNode, 0)
			for vn := range r.IterVirtualNodes() {
				iterList = append(iterList, vn)
				time.Sleep(time.Duration(rand.Int31n(1<<10)) * time.Microsecond)
			}
			if len(iterList) != numNodes*numVnodes {
				t.Errorf("[goroutine-%d] +%s: len(iterList) == %d != %d == numVnodes*numNodes\n",
					workerID, time.Since(start), len(iterList), numVnodes*numNodes)
			} else {
				t.Logf("[goroutine-%d] +%s: The number of iterated virtual nodes looks OK.",
					workerID, time.Since(start))
			}
			done <- struct{}{}
		}(goroutine)
	}

	t.Logf("[main-goroutine] +%s: Waiting for all worker goroutines to finish...\n", time.Since(start))
	for i := 0; i < concurrency; i++ {
		<-done
	}
}
func TestParallelIterTinyRing(t *testing.T) { testParallelIter(t, 3, 4, 4, 10) }
func TestParallelIterBigRing(t *testing.T)  { testParallelIter(t, 2, 128, 128, 15) }

func testParallelIterReversed(t *testing.T, replicationFactor, numVnodes, numNodes, concurrency int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	vns := make([]*VirtualNode, 0)
	for vn := range r.IterVirtualNodes() {
		vns = append(vns, vn)
	}

	done := make(chan struct{})
	runtime.GOMAXPROCS(runtime.NumCPU())
	start := time.Now()
	for goroutine := 0; goroutine < concurrency; goroutine++ {
		go func(workerID int) {
			i := 0
			for vn := range r.IterReversedVirtualNodes() {
				if bytes.Compare(vn.name, vns[len(vns)-1-i].name) != 0 {
					t.Errorf("[goroutine-%d] +%s: reversed[%d] should be %x instead of %x\n",
						workerID, time.Since(start), i, vns[len(vns)-1-i].name, vn.name)
				}
				i++
			}
			done <- struct{}{}
		}(goroutine)
	}

	for i := 0; i < concurrency; i++ {
		<-done
	}
	t.Logf("[goroutine-main]: +%s: All good.\n", time.Since(start))
}
func TestParallelIterReversedTinyRing(t *testing.T) { testParallelIterReversed(t, 3, 4, 4, 10) }
func TestParallelIterReversedBigRing(t *testing.T)  { testParallelIterReversed(t, 2, 128, 128, 15) }

func testVirtualNodeForKey(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	for vn := range r.IterVirtualNodes() {
		if bytes.Compare(r.VirtualNodeForKey(vn.name).name, vn.name) != 0 {
			t.Errorf("VirtualNodeForKey(%x) != %x\n", r.VirtualNodeForKey(vn.name), vn.name)
		}
	}

	lastKey, _ := hex.DecodeString(strings.Repeat("f", 64))
	firstVN := <-r.IterVirtualNodes()
	if bytes.Compare(r.VirtualNodeForKey(lastKey).name, firstVN.name) != 0 {
		t.Errorf("VirtualNodeForKey(%x) != %x\n", r.VirtualNodeForKey(lastKey), firstVN.name)
	}
}
func TestVirtualNodeForKeyTinyRing(t *testing.T)     { testVirtualNodeForKey(t, 2, 2, 2) }
func TestVirtualNodeForKeyMedium1Ring(t *testing.T)  { testVirtualNodeForKey(t, 2, 64, 32) }
func TestVirtualNodeForKeyMedium2Ring(t *testing.T)  { testVirtualNodeForKey(t, 2, 128, 8) }
func TestVirtualNodeForKeyBigRing(t *testing.T)      { testVirtualNodeForKey(t, 2, 128, 128) }
func TestVirtualNodeForKeyHugeRing(t *testing.T)     { testVirtualNodeForKey(t, 2, 256, 512) }
func TestVirtualNodeForKeyGiganticRing(t *testing.T) { testVirtualNodeForKey(t, 2, 512, 1024) }

func TestPredSuccEmptyRing(t *testing.T) {
	r, err := NewHashRing(2, 2)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	vnodeHash := blake2b.Sum256([]byte("node-42"))
	if _, err = r.Predecessor(vnodeHash[:]); err != nil {
		t.Logf("Received error %q, as expected.\n", err.Error())
	} else {
		t.Errorf("Expected error for calling Predecessor() on empty ring.\n")
	}
	if _, err = r.Successor(vnodeHash[:]); err != nil {
		t.Logf("Received error %q, as expected.\n", err.Error())
	} else {
		t.Errorf("Expected error for calling Successor() on empty ring.\n")
	}
}

func TestPredSuccNodeEmptyRing(t *testing.T) {
	r, err := NewHashRing(2, 2)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	vnodeHash := blake2b.Sum256([]byte("node-42"))
	if _, err = r.PredecessorNode(vnodeHash[:]); err != nil {
		t.Logf("Received error %q, as expected.\n", err.Error())
	} else {
		t.Errorf("Expected error for calling PredecessorNode() on empty ring.\n")
	}
	if _, err = r.SuccessorNode(vnodeHash[:]); err != nil {
		t.Logf("Received error %q, as expected.\n", err.Error())
	} else {
		t.Errorf("Expected error for calling SuccessorNode() on empty ring.\n")
	}
}

func TestPredSuccNodeSingleNodeRing(t *testing.T) {
	r, err := NewHashRing(2, 2, "node-42")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	vn := <-r.IterVirtualNodes()
	if _, err := r.PredecessorNode(vn.name); err != nil {
		t.Logf("Received error %q, as expected.\n", err.Error())
	} else {
		t.Errorf("Expected error for calling PredecessorNode() on single-node ring.\n")
	}
	if _, err = r.SuccessorNode(vn.name); err != nil {
		t.Logf("Received error %q, as expected.\n", err.Error())
	} else {
		t.Errorf("Expected error for calling SuccessorNode() on single-node ring.\n")
	}
}

func testPredecessor(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	vns := make([]*VirtualNode, 0)
	for vn := range r.IterVirtualNodes() {
		vns = append(vns, vn)
	}

	reportedPredecessors := make([]*VirtualNode, len(vns))
	for i := 1; i < len(vns)+1; i++ {
		reportedPredecessors[i-1], err = r.Predecessor(vns[i%len(vns)].name)
		if err != nil {
			t.Errorf("Predecessor(%x): %v\n", vns[i%len(vns)].name, err)
		}
	}

	for i, vn := range reportedPredecessors {
		if bytes.Compare(vns[i].name, vn.name) != 0 {
			t.Errorf("reportedPredecessors[%d] == %x != %x == virtualNodes[%d]\n", i, vns[i].name, vn.name, i)
		}
	}
}
func TestPredecessorMedium1Ring(t *testing.T)  { testPredecessor(t, 3, 64, 32) }
func TestPredecessorMedium2Ring(t *testing.T)  { testPredecessor(t, 3, 128, 8) }
func TestPredecessorBigRing(t *testing.T)      { testPredecessor(t, 3, 128, 128) }
func TestPredecessorHugeRing(t *testing.T)     { testPredecessor(t, 3, 256, 512) }
func TestPredecessorGiganticRing(t *testing.T) { testPredecessor(t, 3, 512, 1024) }

func testSuccessor(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	vns := make([]*VirtualNode, 0)
	for vn := range r.IterVirtualNodes() {
		vns = append(vns, vn)
	}

	reportedSuccessors := make([]*VirtualNode, len(vns))
	for i := 0; i < len(vns); i++ {
		reportedSuccessors[i], err = r.Successor(vns[i].name)
		if err != nil {
			t.Errorf("Successor(%x): %v\n", vns[i].name, err)
		}
	}

	for i, vn := range reportedSuccessors {
		if bytes.Compare(vns[(i+1)%len(vns)].name, vn.name) != 0 {
			t.Errorf("reportedSuccessors[%d] == %x != %x == virtualNodes[%d]\n",
				i, vns[(i+1)%len(vns)].name, vn.name, i)
		}
	}
}
func TestSucccessorMedium1Ring(t *testing.T) { testSuccessor(t, 3, 64, 32) }
func TestSuccessorMedium2Ring(t *testing.T)  { testSuccessor(t, 3, 128, 8) }
func TestSuccessorBigRing(t *testing.T)      { testSuccessor(t, 3, 128, 128) }
func TestSuccessorHugeRing(t *testing.T)     { testSuccessor(t, 3, 256, 512) }
func TestSuccessorGiganticRing(t *testing.T) { testSuccessor(t, 3, 512, 1024) }

func testPredecessorNode(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	// Check each virtual node in the current state:
	for vn := range r.IterVirtualNodes() {
		// Get the reported predecessor...
		reportedPredecessor, err := r.PredecessorNode(vn.name)
		if err != nil {
			t.Errorf("PredecessorNode(): %v\n", err)
			t.FailNow()
		}
		// ...and then traverse through state's virtual nodes (in
		// reversed order) to confirm the result.
		confirmed := false
		for pvn, _ := r.Predecessor(vn.name); bytes.Compare(pvn.name, vn.name) != 0; pvn, _ = r.Predecessor(pvn.name) {
			// pvn is loop's auxiliary variable
			// If pvn's node is the same as vn's, continue the
			// reverse traversal.
			if pvn.node != vn.node {
				// When pvn's node is different than vn's, it
				// should be same as the reported.
				if pvn.node == reportedPredecessor.node {
					confirmed = true
					break // to the next virtual node of the state
				} else {
					t.Errorf("PredecessorNode() for %q returned %q; expected %q\n",
						vn.String(), reportedPredecessor.String(), pvn.String())
					t.FailNow()
				}
			}
		}
		if !confirmed {
			t.Errorf("Loop ended; result unconfirmed..\n")
			t.FailNow()
		}
	}
}
func TestPredecessorNodeTinyRing(t *testing.T)     { testPredecessorNode(t, 3, 4, 2) }
func TestPredecessorNodeMedium1Ring(t *testing.T)  { testPredecessorNode(t, 3, 64, 32) }
func TestPredecessorNodeMedium2Ring(t *testing.T)  { testPredecessorNode(t, 3, 128, 8) }
func TestPredecessorNodeBigRing(t *testing.T)      { testPredecessorNode(t, 3, 128, 128) }
func TestPredecessorNodeHugeRing(t *testing.T)     { testPredecessorNode(t, 3, 256, 512) }
func TestPredecessorNodeGiganticRing(t *testing.T) { testPredecessorNode(t, 3, 512, 1024) }

func TestPredecessorNodeNonVnode(t *testing.T) {
	r, err := NewHashRing(2, 4, "node-0", "node-1", "node-2")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	key := blake2b.Sum256([]byte("test"))
	reportedSuccessor, err := r.SuccessorNode(key[:])
	if err != nil {
		t.Errorf("SuccessorNode(): %v\n", err)
		t.FailNow()
	}
	if reportedSuccessor.node != r.NodesForKey(key[:])[1] { // only when replicationFactor == 2
		t.Errorf("SuccessorNode() == %x != %x == NodesForKey()[1]\n",
			reportedSuccessor.node, r.NodesForKey(key[:])[1])
	}
}

func testSuccessorNode(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	for vn := range r.IterVirtualNodes() {
		// Get the reported successor...
		reportedSuccessor, err := r.SuccessorNode(vn.name)
		if err != nil {
			t.Errorf("SuccessorNode(): %v\n", err)
			t.FailNow()
		}
		// ...and then traverse through state's virtual nodes (in
		// reversed order) to confirm the result.
		confirmed := false
		for svn, _ := r.Successor(vn.name); bytes.Compare(svn.name, vn.name) != 0; svn, _ = r.Successor(svn.name) {
			// pvn is loop's auxiliary variable
			// If svn's node is the same as vn's, continue the
			// reverse traversal.
			if svn.node != vn.node {
				// When pvn's node is different than vn's, it
				// should be same as the reported.
				if svn.node == reportedSuccessor.node {
					confirmed = true
					break // to the next virtual node of the state
				} else {
					t.Errorf("SuccessorNode() for %q returned %q; expected %q\n",
						vn.String(), reportedSuccessor.String(), svn.String())
					t.FailNow()
				}
			}
		}
		if !confirmed {
			t.Errorf("Loop ended; result unconfirmed..\n")
			t.FailNow()
		}
	}
}
func TestSuccessorNodeTinyRing(t *testing.T)     { testSuccessorNode(t, 3, 4, 2) }
func TestSuccessorNodeMedium1Ring(t *testing.T)  { testSuccessorNode(t, 3, 64, 32) }
func TestSuccessorNodeMedium2Ring(t *testing.T)  { testSuccessorNode(t, 3, 128, 8) }
func TestSucccessorNodeBigRing(t *testing.T)     { testSuccessorNode(t, 3, 128, 128) }
func TestSuccessorNodeHugeRing(t *testing.T)     { testSuccessorNode(t, 3, 256, 512) }
func TestSuccessorNodeGiganticRing(t *testing.T) { testSuccessorNode(t, 3, 512, 1024) }

func testHasVirtualNode(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	for vn := range r.IterVirtualNodes() {
		// check the virtual node
		if hasVN := r.HasVirtualNode(vn.name); !hasVN {
			t.Errorf("virtual node %q exists, but reported otherwise\n", vn.String())
		}

		// check virtual node's next key in the keyspace
		next := new(big.Int).SetBytes(vn.name)
		next = next.Add(next, big.NewInt(int64(1)))
		nextBytes, _ := hex.DecodeString(fmt.Sprintf("%064x", next))
		succ, err := r.Successor(vn.name) // XXX: tests with numNodes == 1 won't work
		if err != nil {
			t.Errorf("Successor(%x): %v\n", vn.name, err)
			t.FailNow()
		}
		if hasVN := r.HasVirtualNode(nextBytes); hasVN && bytes.Compare(succ.name, nextBytes) != 0 {
			t.Errorf("virtual node %x does not exist, but reported otherwise\n", nextBytes)
		}
	}
}
func TestHasVirtualNodeMedium1Ring(t *testing.T)  { testHasVirtualNode(t, 2, 64, 32) }
func TestHasVirtualNodeMedium2Ring(t *testing.T)  { testHasVirtualNode(t, 2, 128, 8) }
func TestHasVirtualNodeBigRing(t *testing.T)      { testHasVirtualNode(t, 2, 128, 128) }
func TestHasVirtualNodeHugeRing(t *testing.T)     { testHasVirtualNode(t, 2, 256, 512) }
func TestHasVirtualNodeGiganticRing(t *testing.T) { testHasVirtualNode(t, 2, 512, 1024) }

/*
 * BENCHMARKS
 *
 * To run only the benchmarks:
 *	$ go test -v -bench=. -benchtime=9s -benchmem -run=matchnothingregex
 */

func benchmarkNewHashRing(b *testing.B, replicationFactor, numVnodes, numNodes int) *HashRing {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	var r *HashRing
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = NewHashRing(replicationFactor, numVnodes, nodes...)
	}
	return r
}
func BenchmarkNewMedium1Ring(b *testing.B)   { _ = benchmarkNewHashRing(b, 3, 64, 32) }
func BenchmarkNewMedium2Ring(b *testing.B)   { _ = benchmarkNewHashRing(b, 3, 128, 8) }
func BenchmarkNewxBigRing(b *testing.B)      { _ = benchmarkNewHashRing(b, 3, 128, 128) }
func BenchmarkNewxHugeRing(b *testing.B)     { _ = benchmarkNewHashRing(b, 3, 256, 512) }
func BenchmarkNewxGiganticRing(b *testing.B) { _ = benchmarkNewHashRing(b, 3, 512, 1024) }
