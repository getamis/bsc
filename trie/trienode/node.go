// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trienode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/ethereum/go-ethereum/common"
)

const (
	numLevels  = 7
	minHandles = 524288
)

var (
	once       sync.Once
	nodeBlobDB *pebble.DB
	cnt        uint64
	stopGC     bool
	hashNodeDB *pebble.DB
	mu         sync.RWMutex
)

func InitNodeBlobDB(name string, inMemory bool) error {
	// Already initialized
	if nodeBlobDB != nil {
		return nil
	}

	// Initialize the database
	db, err := initDB(name, inMemory)
	if err != nil {
		return err
	}
	nodeBlobDB = db
	return nil
}

func InitHashNodeDB(name string, inMemory bool) error {
	// Already initialized
	if hashNodeDB != nil {
		return nil
	}

	// Initialize the database
	db, err := initDB(name, inMemory)
	if err != nil {
		return err
	}
	hashNodeDB = db
	return nil
}

func initDB(name string, inMemory bool) (*pebble.DB, error) {
	// Use in-memory DB if no name is provided
	if name == "" && !inMemory {
		inMemory = true
	}

	opt := &pebble.Options{
		Cache:                    pebble.NewCache(int64(1024 * 1024 * 1024)),
		MaxOpenFiles:             minHandles,
		MemTableSize:             64 << 20,
		MaxConcurrentCompactions: runtime.NumCPU,
		DisableWAL:               true,
		Levels:                   make([]pebble.LevelOptions, numLevels),
	}
	for i := 0; i < len(opt.Levels); i++ {
		l := &opt.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opt.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opt.Experimental.ReadSamplingMultiplier = -1
	if inMemory {
		opt.FS = vfs.NewMem()
		name = "" // Empty name for in-memory DB
	} else {
		if err := os.RemoveAll(name); err != nil {
			return nil, err
		}
	}

	db, err := pebble.Open(name, opt)
	return db, err
}

func DisableGCForDB() {
	stopGC = true
}

func CompactNodeBlobDB() {
	if testing.Testing() {
		return
	}
	start := make([]byte, 8)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, cnt)
	_ = nodeBlobDB.Compact(start, end, true)
}

func Get(hash common.Hash) []byte {
	mu.RLock()
	defer mu.RUnlock()

	iter, _ := hashNodeDB.NewIter(nil)
	defer iter.Close()

	if iter.SeekGE(hash.Bytes()); iter.Valid() {
		hashKey := iter.Key()
		if !bytes.Equal(hashKey[:common.HashLength], hash.Bytes()) {
			return nil
		}

		value, _ := iter.ValueAndErr()
		key := make([]byte, len(value))
		copy(key, value)

		// Get the node blob from the database
		return getByKey(key)
	}

	return nil
}

func getByKey(key []byte) []byte {
	value, closer, _ := nodeBlobDB.Get(key)
	ret := make([]byte, len(value))
	copy(ret, value)
	_ = closer.Close()
	return ret
}

// Node is a wrapper which contains the encoded blob of the trie node and its
// node hash. It is general enough that can be used to represent trie node
// corresponding to different trie implementations.
type Node struct {
	Hash common.Hash // Node hash, empty for deleted node
	Len  int
	idx  uint64
}

func (n *Node) Blob() []byte {
	if n.IsDeleted() {
		return nil
	}

	return getByKey(n.key())
}

type HashBlob struct {
	Hash common.Hash
	Blob []byte
}

const bulkGetNodesConcurrency = 256

func BulkGetNodes(nodes []*Node) map[common.Hash][]byte {
	ret := make(map[common.Hash][]byte)
	nodeCh := make(chan *Node, len(nodes))
	resultCh := make(chan *HashBlob, len(nodes))
	var wg sync.WaitGroup

	for i := 0; i < min(bulkGetNodesConcurrency, len(nodes)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for node := range nodeCh {
				resultCh <- &HashBlob{Hash: node.Hash, Blob: node.Blob()}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for _, node := range nodes {
		nodeCh <- node
	}
	close(nodeCh)

	for hashBlob := range resultCh {
		ret[hashBlob.Hash] = hashBlob.Blob
	}
	return ret
}

// Size returns the total memory size used by this node.
func (n *Node) Size() int {
	return n.Len + common.HashLength
}

// IsDeleted returns the indicator if the node is marked as deleted.
func (n *Node) IsDeleted() bool {
	return n.Len == 0
}

func (n *Node) key() []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, n.idx)
	return key
}

func (n *Node) hashKey() []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, n.idx)
	key = append(n.Hash.Bytes(), key...)
	return key
}

// New constructs a node with provided node information.
func New(hash common.Hash, blob []byte) *Node {
	// Initialize the database for testing
	once.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	// Increase the counter
	idx := atomic.AddUint64(&cnt, 1)
	n := &Node{Hash: hash, Len: len(blob), idx: idx}

	if n.IsDeleted() {
		return n
	}

	// Insert the node blob into the database
	_ = nodeBlobDB.Set(n.key(), blob, pebble.NoSync)

	// Insert the hash into the database
	_ = hashNodeDB.Set(n.hashKey(), n.key(), pebble.NoSync)

	// Set the finalizer to delete the node blob from the database
	runtime.SetFinalizer(n, func(n *Node) {
		if stopGC {
			return
		}

		mu.Lock()
		defer mu.Unlock()
		_ = nodeBlobDB.Delete(n.key(), pebble.NoSync)
		_ = hashNodeDB.Delete(n.hashKey(), pebble.NoSync)
	})
	return n
}

// NewDeleted constructs a node which is deleted.
func NewDeleted() *Node { return New(common.Hash{}, nil) }

// leaf represents a trie leaf node
type leaf struct {
	Blob   []byte      // raw blob of leaf
	Parent common.Hash // the hash of parent node
}

// NodeSet contains a set of nodes collected during the commit operation.
// Each node is keyed by path. It's not thread-safe to use.
type NodeSet struct {
	Owner   common.Hash
	Leaves  []*leaf
	Nodes   map[string]*Node
	updates int // the count of updated and inserted nodes
	deletes int // the count of deleted nodes
}

// NewNodeSet initializes a node set. The owner is zero for the account trie and
// the owning account address hash for storage tries.
func NewNodeSet(owner common.Hash) *NodeSet {
	return &NodeSet{
		Owner: owner,
		Nodes: make(map[string]*Node),
	}
}

// ForEachWithOrder iterates the nodes with the order from bottom to top,
// right to left, nodes with the longest path will be iterated first.
func (set *NodeSet) ForEachWithOrder(callback func(path string, n *Node)) {
	paths := make([]string, 0, len(set.Nodes))
	for path := range set.Nodes {
		paths = append(paths, path)
	}
	// Bottom-up, the longest path first
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, path := range paths {
		callback(path, set.Nodes[path])
	}
}

// AddNode adds the provided node into set.
func (set *NodeSet) AddNode(path []byte, n *Node) {
	if n.IsDeleted() {
		set.deletes += 1
	} else {
		set.updates += 1
	}
	set.Nodes[string(path)] = n
}

// MergeSet merges this 'set' with 'other'. It assumes that the sets are disjoint,
// and thus does not deduplicate data (count deletes, dedup leaves etc).
func (set *NodeSet) MergeSet(other *NodeSet) error {
	if set.Owner != other.Owner {
		return fmt.Errorf("nodesets belong to different owner are not mergeable %x-%x", set.Owner, other.Owner)
	}
	maps.Copy(set.Nodes, other.Nodes)

	set.deletes += other.deletes
	set.updates += other.updates

	// Since we assume the sets are disjoint, we can safely append leaves
	// like this without deduplication.
	set.Leaves = append(set.Leaves, other.Leaves...)
	return nil
}

// Merge adds a set of nodes into the set.
func (set *NodeSet) Merge(owner common.Hash, nodes map[string]*Node) error {
	if set.Owner != owner {
		return fmt.Errorf("nodesets belong to different owner are not mergeable %x-%x", set.Owner, owner)
	}
	for path, node := range nodes {
		prev, ok := set.Nodes[path]
		if ok {
			// overwrite happens, revoke the counter
			if prev.IsDeleted() {
				set.deletes -= 1
			} else {
				set.updates -= 1
			}
		}
		if node.IsDeleted() {
			set.deletes += 1
		} else {
			set.updates += 1
		}
		set.Nodes[path] = node
	}
	return nil
}

// AddLeaf adds the provided leaf node into set. TODO(rjl493456442) how can
// we get rid of it?
func (set *NodeSet) AddLeaf(parent common.Hash, blob []byte) {
	set.Leaves = append(set.Leaves, &leaf{Blob: blob, Parent: parent})
}

// Size returns the number of dirty nodes in set.
func (set *NodeSet) Size() (int, int) {
	return set.updates, set.deletes
}

// HashSet returns a set of trie nodes keyed by node hash.
func (set *NodeSet) HashSet() map[common.Hash][]byte {
	ret := make(map[common.Hash][]byte, len(set.Nodes))
	for _, n := range set.Nodes {
		ret[n.Hash] = n.Blob()
	}
	return ret
}

// Summary returns a string-representation of the NodeSet.
func (set *NodeSet) Summary() string {
	var out = new(strings.Builder)
	fmt.Fprintf(out, "nodeset owner: %v\n", set.Owner)
	for path, n := range set.Nodes {
		// Deletion
		if n.IsDeleted() {
			fmt.Fprintf(out, "  [-]: %x\n", path)
			continue
		}
		// Insertion or update
		fmt.Fprintf(out, "  [+/*]: %x -> %v \n", path, n.Hash)
	}
	for _, n := range set.Leaves {
		fmt.Fprintf(out, "[leaf]: %v\n", n)
	}
	return out.String()
}

// MergedNodeSet represents a merged node set for a group of tries.
type MergedNodeSet struct {
	Sets map[common.Hash]*NodeSet
}

// NewMergedNodeSet initializes an empty merged set.
func NewMergedNodeSet() *MergedNodeSet {
	return &MergedNodeSet{Sets: make(map[common.Hash]*NodeSet)}
}

// NewWithNodeSet constructs a merged nodeset with the provided single set.
func NewWithNodeSet(set *NodeSet) *MergedNodeSet {
	merged := NewMergedNodeSet()
	merged.Merge(set)
	return merged
}

// Merge merges the provided dirty nodes of a trie into the set. The assumption
// is held that no duplicated set belonging to the same trie will be merged twice.
func (set *MergedNodeSet) Merge(other *NodeSet) error {
	subset, present := set.Sets[other.Owner]
	if present {
		return subset.Merge(other.Owner, other.Nodes)
	}
	set.Sets[other.Owner] = other
	return nil
}

// Flatten returns a two-dimensional map for internal nodes.
func (set *MergedNodeSet) Flatten() map[common.Hash]map[string]*Node {
	nodes := make(map[common.Hash]map[string]*Node, len(set.Sets))
	for owner, set := range set.Sets {
		nodes[owner] = set.Nodes
	}
	return nodes
}
