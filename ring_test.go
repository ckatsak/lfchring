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
	"crypto/sha256"
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
	//"golang.org/x/crypto/blake2b"
)

var (
	//blake2bHash = func(in []byte) []byte {
	//	out := blake2b.Sum256(in)
	//	return out[:]
	//}

	sha256Hash = func(in []byte) []byte {
		out := sha256.Sum256(in)
		return out[:]
	}

	hashFunc = sha256Hash

	r *HashRing
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
	if numVnodes != r.Size()*int(state.virtualNodeCount) {
		t.Errorf("Counted %d virtual nodes; expected %d.\n", numVnodes, r.Size()*int(state.virtualNodeCount))
	}
	t.Log("Ring state's slice of virtual nodes looks OK.")
}

func TestNewRingBadValues(t *testing.T) {
	if _, err := NewHashRing(nil, 5, 5); err != nil {
		t.Logf("NewHashRing(nil, 5, 5): %v\n", err)
	} else {
		t.Errorf("Expected error from NewHashRing()\n")
	}
	if _, err := NewHashRing(hashFunc, 1<<8, 1<<16); err != nil {
		t.Logf("NewHashRing(hashFunc, 1<<8, 1<<16): %v\n", err)
	} else {
		t.Errorf("Expected error from NewHashRing()\n")
	}
	if _, err := NewHashRing(hashFunc, 1<<8-1, 1<<16); err != nil {
		t.Logf("NewHashRing(hashFunc, 1<<8-1, 1<<16): %v\n", err)
	} else {
		t.Errorf("Expected error from NewHashRing()\n")
	}
}

func TestNewEmptyRing(t *testing.T) {
	r, err := NewHashRing(hashFunc, 3, 4)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	if r.Size() != 0 {
		t.Errorf("Size of empty ring was reported to be %d.\n", r.Size())
	}
}

func TestNewRingReplicationFactorLessThanVirtualNodeCount(t *testing.T) {
	r, err := NewHashRing(hashFunc, 3, 2, "node-0", "node-1")
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
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

var garbageStr string

func TestStringGiganticRing(t *testing.T) {
	replicationFactor := 4
	numVnodes := 512
	numNodes := 1024

	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	garbageStr = r.String()
	t.Log("Size of ring's representation:", len(garbageStr), "bytes.")
}

func testClone(t *testing.T, replicationFactor, virtualNodeCount, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	oldRing, err := NewHashRing(hashFunc, replicationFactor, virtualNodeCount, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	checkVirtualNodes(t, oldRing)

	newRing := oldRing.Clone()
	checkVirtualNodes(t, newRing)

	// check size
	if oldRing.Size() != newRing.Size() {
		t.Errorf("oldRing.Size() == %d != %d == newRing.Size()", oldRing.Size(), newRing.Size())
		t.FailNow()
	} else {
		t.Log("Sizes OK.")
	}

	// check virtual nodes
	oldVNs := oldRing.VirtualNodes(nil)
	newVNs := newRing.VirtualNodes(nil)
	for i := 0; i < oldRing.Size()*virtualNodeCount; i++ {
		oldNext := <-oldVNs
		newNext := <-newVNs

		if bytes.Compare(oldNext.name, newNext.name) != 0 {
			t.Errorf("%#v != %#v", oldNext, newNext)
			t.FailNow()
		}

		oldNodes := oldRing.NodesForKey(oldNext.name)
		newNodes := newRing.NodesForKey(newNext.name)
		for i, oldNode := range oldNodes {
			newNode := newNodes[i]
			if newNode != oldNode {
				t.Errorf("oldNodes[%d] == %s != %s == newNodes[%d]", i, oldNode, newNode, i)
				t.FailNow()
			}
		}
	}
	// Make sure channels are drained.
	if oldNext := <-oldVNs; oldNext != nil {
		t.Errorf("Something is wrong with the iteration channel.")
		t.FailNow()
	}
	if newNext := <-newVNs; newNext != nil {
		t.Errorf("New ring also has this: %#v", newNext)
		t.FailNow()
	}
	t.Log("Virtual nodes OK.")
}
func TestCloneTinyRing(t *testing.T)     { testClone(t, 3, 4, 2) }
func TestCloneMedium1Ring(t *testing.T)  { testClone(t, 3, 64, 32) }
func TestCloneMedium2Ring(t *testing.T)  { testClone(t, 3, 128, 8) }
func TestCloneBigRing(t *testing.T)      { testClone(t, 4, 128, 128) }
func TestCloneHugeRing(t *testing.T)     { testClone(t, 3, 256, 512) }
func TestCloneGiganticRing(t *testing.T) { testClone(t, 3, 256, 1024) }

/*
//FIXME: reflect.DeepEqual() is not helpful in our case, because replicaOwners
//maps are not deeply equal because of the XXX below. See godoc for more
//information.
func TestCloneDeepEqual(t *testing.T) {
	r, err := NewHashRing(hashFunc, 2, 4, "node-0", "node-1")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	r2 := r.Clone()
	if !reflect.DeepEqual(r, r2) {
		t.Errorf("*HashRing.Clone() malfunction.")
		t.Log("state reflect.DeepEqual():", reflect.DeepEqual(r.state, r2.state))
		t.Log("state.Load().(*hashRingState) reflect.DeepEqual():",
			reflect.DeepEqual(r.state.Load().(*hashRingState), r2.state.Load().(*hashRingState)))
		t.Log("state.Load().(*hashRingState).numVirtualNodes reflect.DeepEqual():",
			reflect.DeepEqual(
				r.state.Load().(*hashRingState).numVirtualNodes,
				r2.state.Load().(*hashRingState).numVirtualNodes,
			),
		)
		t.Log("state.Load().(*hashRingState).replicationFactor reflect.DeepEqual():",
			reflect.DeepEqual(
				r.state.Load().(*hashRingState).replicationFactor,
				r2.state.Load().(*hashRingState).replicationFactor,
			),
		)
		t.Log("state.Load().(*hashRingState).virtualNodes reflect.DeepEqual():",
			reflect.DeepEqual(
				r.state.Load().(*hashRingState).virtualNodes,
				r2.state.Load().(*hashRingState).virtualNodes,
			),
		)
		t.Log("state.Load().(*hashRingState).replicaOwners reflect.DeepEqual():",
			reflect.DeepEqual(
				r.state.Load().(*hashRingState).replicaOwners,
				r2.state.Load().(*hashRingState).replicaOwners,
			),
		)
		rFirst := <-r.VirtualNodes(nil)
		r2First := <-r2.VirtualNodes(nil)
		t.Log("state.Load().(*hashRingState).replicaOwners[first] reflect.DeepEqual():",
			reflect.DeepEqual(
				r.state.Load().(*hashRingState).replicaOwners[rFirst],
				r2.state.Load().(*hashRingState).replicaOwners[r2First],
			),
		)
		t.Log("state.Load().(*hashRingState).replicaOwners key [first] reflect.DeepEqual():",
			reflect.DeepEqual(
				rFirst,
				r2First,
			),
		)
		t.Log("rFirst == r2First :", rFirst == r2First) // XXX
		for k, v := range r.state.Load().(*hashRingState).replicaOwners {
			if v2, exists2 := r2.state.Load().(*hashRingState).replicaOwners[k]; !exists2 {
				t.Errorf("Key {%v} not present in r2!\n\n", k)
			} else if !reflect.DeepEqual(v, v2) {
				t.Errorf("This is not equal:\nKey: {%#v} -->\nv1: {%#v}\nv2: {%#v}\n\n", k, v, v2)
			}
		}

		t.Logf("%#v", r)
		t.Logf("%#v", r2)
		t.Logf("%#v", r.state.Load().(*hashRingState).replicaOwners)
		t.Logf("%#v", r2.state.Load().(*hashRingState).replicaOwners)

		//t.Log("r:\n", r)
		t.Log("r2:\n", r2)
		//r.Add("node-2")
		//t.Log("r (later):\n", r)
	}
}
*/

func TestAddExistingNode(t *testing.T) {
	r, err := NewHashRing(hashFunc, 2, 4, "node-0", "node-1")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	_, err = r.Add("node-0")
	if err != nil {
		t.Logf("Add(): returned error %q, as expected\n", err.Error())
	} else {
		t.Errorf("Add(): Expected an error for adding an existing node\n")
	}
}

func testAdd(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	// Create the ring
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	checkVirtualNodes(t, r)

	// readers
	start := time.Now()
	done := make(chan struct{})
	for goroutine := 0; goroutine < concurrency; goroutine++ {
		go func(workerID int) {
			readCnt := 0
			size := 0
			for {
				select {
				case <-done:
					return
				case <-time.After(20 * time.Millisecond):
					newSize := r.Size()
					readCnt++
					if newSize < size {
						t.Errorf("[reader-%d] +%s: newSize < size", workerID, time.Since(start))
					} else {
						t.Logf("[reader-%d] +%s: OK after reading %d time(s). (%d->%d)",
							workerID, time.Since(start), readCnt, size, newSize)
					}
					size = newSize
				}
			}
		}(goroutine)
	}

	// writer
	for i := numNodes; i < numNodes+(numNodes/10); i++ {
		<-time.After(30 * time.Millisecond)
		if _, err := r.Add(Node(fmt.Sprintf("node-%d", i))); err != nil {
			t.Errorf("[writer] +%s: Add(\"node-%d\"): %v\n", time.Since(start), i, err)
		} else {
			checkVirtualNodes(t, r)
			t.Logf("[writer] +%s: OK after adding %d node(s).", time.Since(start), i+1)
		}
	}
	<-time.After(30 * time.Millisecond)
	close(done)
}
func TestParallelRWTinyRing(t *testing.T)    { testParallelRW(t, 3, 4, 4, 10) }
func TestParallelRWMedium1Ring(t *testing.T) { testParallelRW(t, 3, 64, 32, 10) }
func TestParallelRWMedium2Ring(t *testing.T) { testParallelRW(t, 3, 128, 8, 10) }
func TestParallelRWBigRing(t *testing.T)     { testParallelRW(t, 3, 128, 128, 10) }
func TestParallelRWHugeRing(t *testing.T)    { testParallelRW(t, 3, 64, 256, 10) }

// NOTE: The ring is not expected to be able to handle multiple writers.
func testParallelAdd(t *testing.T, replicationFactor, numVnodes, numNodes, concurrency int) {
	// Create the ring
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
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
	r, err := NewHashRing(hashFunc, 2, 8, "node-0", "node-1", "node-2")
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

func testRemoveFromRing(t *testing.T, replicationFactor, virtualNodeCount, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, virtualNodeCount, nodes...)
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
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

	r, err := NewHashRing(hashFunc, 3, 3)
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
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
		keyB := hashFunc(objB)
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

func TestIterStop(t *testing.T) {
	r, err := NewHashRing(hashFunc, 2, 4, "node-0", "node-1")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	stop := make(chan struct{})
	defer close(stop)
	vns := r.VirtualNodes(stop)
	for i := 0; i < r.Size(); i++ {
		<-vns
	}
}

func testIter(t *testing.T, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	iterVNList := make([]*VirtualNode, 0)
	for vn := range r.VirtualNodes(nil) {
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
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
			for vn := range r.VirtualNodes(nil) {
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

func TestIterReversedStop(t *testing.T) {
	r, err := NewHashRing(hashFunc, 2, 4, "node-0", "node-1")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	stop := make(chan struct{})
	defer close(stop)
	vns := r.VirtualNodesReversed(stop)
	for i := 0; i < r.Size(); i++ {
		<-vns
	}
}
func testParallelIterReversed(t *testing.T, replicationFactor, numVnodes, numNodes, concurrency int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	vns := make([]*VirtualNode, 0)
	for vn := range r.VirtualNodes(nil) {
		vns = append(vns, vn)
	}

	done := make(chan struct{})
	runtime.GOMAXPROCS(runtime.NumCPU())
	start := time.Now()
	for goroutine := 0; goroutine < concurrency; goroutine++ {
		go func(workerID int) {
			stop := make(chan struct{})
			defer close(stop)
			i := 0
			for vn := range r.VirtualNodesReversed(stop) {
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	stop := make(chan struct{})
	defer close(stop)
	for vn := range r.VirtualNodes(stop) {
		if bytes.Compare(r.VirtualNodeForKey(vn.name).name, vn.name) != 0 {
			t.Errorf("VirtualNodeForKey(%x) != %x\n", r.VirtualNodeForKey(vn.name), vn.name)
		}
	}

	lastKey, _ := hex.DecodeString(strings.Repeat("f", 64))
	firstVN := <-r.VirtualNodes(nil)
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
	r, err := NewHashRing(hashFunc, 2, 2)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	vnodeHash := hashFunc([]byte("node-42"))
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
	r, err := NewHashRing(hashFunc, 2, 2)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	vnodeHash := hashFunc([]byte("node-42"))
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
	r, err := NewHashRing(hashFunc, 2, 2, "node-42")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}
	vn := <-r.VirtualNodes(nil)
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	vns := make([]*VirtualNode, 0)
	for vn := range r.VirtualNodes(nil) {
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	vns := make([]*VirtualNode, 0)
	for vn := range r.VirtualNodes(nil) {
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	// Check each virtual node in the current state:
	stop := make(chan struct{})
	defer close(stop)
	for vn := range r.VirtualNodes(stop) {
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
	r, err := NewHashRing(hashFunc, 2, 4, "node-0", "node-1", "node-2")
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	key := hashFunc([]byte("test"))
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	stop := make(chan struct{})
	defer close(stop)
	for vn := range r.VirtualNodes(stop) {
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
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		t.Errorf("NewHashRing(): %v\n", err)
		t.FailNow()
	}

	stop := make(chan struct{})
	defer close(stop)
	for vn := range r.VirtualNodes(stop) {
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

func benchmarkNewHashRing(b *testing.B, hash func([]byte) []byte, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = NewHashRing(hash, replicationFactor, numVnodes, nodes...)
	}
}

//func BenchmarkNewMedium1RingBlake2b(b *testing.B)  { benchmarkNewHashRing(b, blake2bHash, 3, 64, 32) }
//func BenchmarkNewMedium1RingSha256(b *testing.B)   { benchmarkNewHashRing(b, sha256Hash, 3, 64, 32) }
func BenchmarkNewMedium1Ring(b *testing.B) { benchmarkNewHashRing(b, hashFunc, 3, 64, 32) }

//func BenchmarkNewMedium2RingSha256(b *testing.B)   { benchmarkNewHashRing(b, blake2bHash, 3, 128, 8) }
//func BenchmarkNewMedium2RingSha256(b *testing.B)   { benchmarkNewHashRing(b, sha256Hash, 3, 128, 8) }
func BenchmarkNewMedium2Ring(b *testing.B) { benchmarkNewHashRing(b, hashFunc, 3, 128, 8) }

//func BenchmarkNewBigRingSha256(b *testing.B)       { benchmarkNewHashRing(b, blake2bHash, 3, 128, 128) }
//func BenchmarkNewBigRingSha256(b *testing.B)       { benchmarkNewHashRing(b, sha256Hash, 3, 128, 128) }
func BenchmarkNewBigRing(b *testing.B) { benchmarkNewHashRing(b, hashFunc, 3, 128, 128) }

//func BenchmarkNewHugeRingSha256(b *testing.B)      { benchmarkNewHashRing(b, blake2bHash, 3, 256, 512) }
//func BenchmarkNewHugeRingSha256(b *testing.B)      { benchmarkNewHashRing(b, sha256Hash, 3, 256, 512) }
func BenchmarkNewHugeRing(b *testing.B) { benchmarkNewHashRing(b, hashFunc, 3, 256, 512) }

//func BenchmarkNewGiganticRingSha256(b *testing.B)  { benchmarkNewHashRing(b, blake2bHash, 3, 512, 1024) }
//func BenchmarkNewGiganticRingSha256(b *testing.B)  { benchmarkNewHashRing(b, sha256Hash, 3, 512, 1024) }
func BenchmarkNewGiganticRing(b *testing.B) { benchmarkNewHashRing(b, hashFunc, 3, 512, 1024) }

func benchmarkString(b *testing.B, replicationFactor, numVnodes, numNodes int) {
	nodes := make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i))
	}
	r, err := NewHashRing(hashFunc, replicationFactor, numVnodes, nodes...)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		garbageStr = r.String()
	}
}
func BenchmarkStringMedium1Ring(b *testing.B) { benchmarkString(b, 3, 64, 32) }
func BenchmarkStringMedium2Ring(b *testing.B) { benchmarkString(b, 3, 128, 8) }
func BenchmarkStringBigRing(b *testing.B)     { benchmarkString(b, 3, 128, 128) }
