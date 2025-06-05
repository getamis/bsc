package pathdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

var (
	onceInitNodeSetDB sync.Once
	nodeBlobDB        *pebble.DB
	hashNodeDB        *pebble.DB
	stopGC            bool
	mu                sync.RWMutex
)

const (
	numLevels  = 7
	minHandles = 524288
)

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

func disableGCForDB() {
	stopGC = true
}

// nodeBlobKey: [block(8)][root(32)][owner(32)][path] => blob
// hashNodeKey: [hash(32)][root(32)] => [block(8)][root(32)][owner(32)][path]

func setNodeSet(block uint64, root common.Hash, nodes *nodeSet) error {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	nodeBlobBatch := nodeBlobDB.NewBatch()
	hashNodeBatch := hashNodeDB.NewBatch()

	for owner, subset := range nodes.nodes {
		for path, node := range subset {
			_ = setNodeBlob(block, root, owner, path, node.Hash, node.Blob, nodeBlobBatch, hashNodeBatch)
		}
	}

	_ = nodeBlobBatch.Commit(pebble.NoSync)
	_ = hashNodeBatch.Commit(pebble.NoSync)
	_ = nodeBlobBatch.Close()
	_ = hashNodeBatch.Close()
	return nil
}

func getNodeSet(block uint64, root common.Hash) (*nodeSet, error) {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	nodes := make(map[common.Hash]map[string]*trienode.Node)

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, block)
	prefix = append(prefix, root[:]...)
	iter, _ := nodeBlobDB.NewIter(nil)
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}

		value, _ := iter.ValueAndErr()
		blob := make([]byte, len(value))
		copy(blob, value)

		owner := common.BytesToHash(iter.Key()[8+common.HashLength : 8+common.HashLength+common.HashLength])
		path := string(iter.Key()[8+common.HashLength+common.HashLength:])
		if _, ok := nodes[owner]; !ok {
			nodes[owner] = make(map[string]*trienode.Node)
		}
		if len(blob) > 0 {
			nodes[owner][path] = trienode.New(crypto.Keccak256Hash(blob), blob)
		} else {
			nodes[owner][path] = trienode.NewDeleted()
		}
	}
	return newNodeSet(nodes), nil
}

func deleteNodeSet(block uint64, root common.Hash) error {
	if stopGC {
		return nil
	}

	onceInitNodeSetDB.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	nodeBlobBatch := nodeBlobDB.NewBatch()
	hashNodeBatch := hashNodeDB.NewBatch()

	nodes, err := getNodeSet(block, root)
	if err != nil {
		return err
	}

	for owner, subset := range nodes.nodes {
		for path, node := range subset {
			_ = deleteNodeBlob(block, root, owner, path, node.Hash, nodeBlobBatch, hashNodeBatch)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	_ = nodeBlobBatch.Commit(pebble.NoSync)
	_ = hashNodeBatch.Commit(pebble.NoSync)
	_ = nodeBlobBatch.Close()
	_ = hashNodeBatch.Close()
	return nil
}

func setNodeBlob(block uint64, root common.Hash, owner common.Hash, path string, hash common.Hash, blob []byte, nodeBlobBatch, hashNodeBatch *pebble.Batch) error {
	nodeBlobKey := getNodeBlobKey(block, root, owner, path)
	hashNodeKey := append(hash[:], root[:]...)

	if err := nodeBlobBatch.Set(nodeBlobKey, blob, pebble.NoSync); err != nil {
		return err
	}

	if hash == (common.Hash{}) {
		// If the hash is empty, we don't need to delete the hashNodeKey
		return nil
	}
	if err := hashNodeBatch.Set(hashNodeKey, nodeBlobKey, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func getNodeBlob(hash common.Hash) []byte {
	mu.RLock()
	defer mu.RUnlock()

	onceInitNodeSetDB.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	iter, _ := hashNodeDB.NewIter(nil)
	defer iter.Close()

	if iter.SeekGE(hash.Bytes()); !iter.Valid() {
		return nil
	}

	hashKey := iter.Key()
	if !bytes.Equal(hashKey[:common.HashLength], hash[:]) {
		return nil
	}

	value, _ := iter.ValueAndErr()
	nodeBlobKey := make([]byte, len(value))
	copy(nodeBlobKey, value)

	blob, closer, err := nodeBlobDB.Get(nodeBlobKey)
	if err != nil {
		return nil
	}
	ret := make([]byte, len(blob))
	copy(ret, blob)
	_ = closer.Close()
	return ret
}

func deleteNodeBlob(block uint64, root common.Hash, owner common.Hash, path string, hash common.Hash, nodeBlobBatch, hashNodeBatch *pebble.Batch) error {
	nodeBlobKey := getNodeBlobKey(block, root, owner, path)
	hashNodeKey := append(hash[:], root[:]...)

	if err := nodeBlobBatch.Delete(nodeBlobKey, pebble.NoSync); err != nil {
		return err
	}

	if hash == (common.Hash{}) {
		// If the hash is empty, we don't need to delete the hashNodeKey
		return nil
	}
	if err := hashNodeBatch.Delete(hashNodeKey, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func getNodeBlobKey(block uint64, root common.Hash, owner common.Hash, path string) []byte {
	blockByte := make([]byte, 8)
	binary.BigEndian.PutUint64(blockByte, block)
	nodeBlobKey := blockByte[:]
	nodeBlobKey = append(nodeBlobKey, root[:]...)
	nodeBlobKey = append(nodeBlobKey, owner[:]...)
	nodeBlobKey = append(nodeBlobKey, []byte(path)...)
	return nodeBlobKey
}
