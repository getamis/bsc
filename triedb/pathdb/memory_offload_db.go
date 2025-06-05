package pathdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/afero"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	onceInitNodeSetDB  sync.Once
	nodeSetDB          afero.Fs
	onceInitNodeBlobDB sync.Once
	nodeBlobDB         *pebble.DB
	hashNodeDB         *pebble.DB
	stopGC             bool
	mu                 sync.RWMutex
)

const (
	numLevels  = 7
	minHandles = 524288
)

func InitNodeSetDB(name string, inMemory bool) error {
	// Already initialized
	if nodeSetDB != nil {
		return nil
	}

	// Initialize the database
	if inMemory {
		nodeSetDB = afero.NewMemMapFs()
		return nil
	}

	nodeSetDB = afero.NewBasePathFs(afero.NewOsFs(), name)
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

func getNodeSetFilename(root common.Hash) string {
	return hex.EncodeToString(root[:])
}

func setNodeSet(root common.Hash, nodes *nodeSet) error {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	var buf bytes.Buffer
	err := nodes.encode(&buf)
	if err != nil {
		return err
	}

	filename := getNodeSetFilename(root)
	return afero.WriteFile(nodeSetDB, filename, buf.Bytes(), 0644)
}

func getNodeSet(root common.Hash) (*nodeSet, error) {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	filename := getNodeSetFilename(root)
	data, err := afero.ReadFile(nodeSetDB, filename)
	if err != nil {
		return nil, err
	}

	buf := rlp.NewStream(bytes.NewReader(data), 0)
	nodes := &nodeSet{}
	if err := nodes.decode(buf); err != nil {
		return nil, err
	}

	return nodes, nil
}

func getNodeSetForWriter(root common.Hash, w io.Writer) error {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	filename := getNodeSetFilename(root)
	data, err := afero.ReadFile(nodeSetDB, filename)
	if err != nil {
		return nil
	}

	_, err = w.Write(data)
	return err
}

func deleteNodeSet(root common.Hash) error {
	if stopGC {
		return nil
	}

	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	filename := getNodeSetFilename(root)
	return nodeSetDB.Remove(filename)
}

func setNodeBlobs(block uint64, root common.Hash, nodes *nodeSet) error {
	onceInitNodeBlobDB.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	nodeBlobBatch := nodeBlobDB.NewBatch()
	hashNodeBatch := hashNodeDB.NewBatch()

	for _, subset := range nodes.nodes {
		for _, node := range subset {
			_ = setNodeBlob(block, root, node.Hash, node.Blob, nodeBlobBatch, hashNodeBatch)
		}
	}

	_ = nodeBlobBatch.Commit(pebble.NoSync)
	_ = hashNodeBatch.Commit(pebble.NoSync)
	_ = nodeBlobBatch.Close()
	_ = hashNodeBatch.Close()
	return nil
}

// nodeBlobKey: [block(8)][root(32)][hash(32)] => blob
// hashNodeKey: [hash(32)][root(32)] => [block(8)]

func setNodeBlob(block uint64, root common.Hash, hash common.Hash, blob []byte, nodeBlobBatch, hashNodeBatch *pebble.Batch) error {
	blockByte := make([]byte, 8)
	binary.BigEndian.PutUint64(blockByte, block)
	nodeBlobKey := blockByte[:]
	nodeBlobKey = append(nodeBlobKey, root[:]...)
	nodeBlobKey = append(nodeBlobKey, hash[:]...)
	hashNodeKey := append(hash[:], root[:]...)

	if err := nodeBlobBatch.Set(nodeBlobKey, blob, pebble.NoSync); err != nil {
		return err
	}
	if err := hashNodeBatch.Set(hashNodeKey, blockByte, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func getNodeBlob(hash common.Hash) []byte {
	mu.RLock()
	defer mu.RUnlock()

	onceInitNodeBlobDB.Do(func() {
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
	root := hashKey[common.HashLength:]
	nodeBlobKey = append(nodeBlobKey, root[:]...)
	nodeBlobKey = append(nodeBlobKey, hash[:]...)

	blob, closer, err := nodeBlobDB.Get(nodeBlobKey)
	if err != nil {
		return nil
	}
	ret := make([]byte, len(blob))
	copy(ret, blob)
	_ = closer.Close()
	return ret
}

func deleteNodeBlobs(block uint64, root common.Hash) error {
	if stopGC {
		return nil
	}

	onceInitNodeBlobDB.Do(func() {
		_ = InitNodeBlobDB("", true)
		_ = InitHashNodeDB("", true)
	})

	iter, _ := nodeBlobDB.NewIter(nil)
	defer iter.Close()

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, block)
	prefix = append(prefix, root[:]...)
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}

		err := deleteNodeBlob(root, iter.Key())
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteNodeBlob(root common.Hash, nodeBlobKey []byte) error {
	mu.Lock()
	defer mu.Unlock()

	if err := nodeBlobDB.Delete(nodeBlobKey, pebble.NoSync); err != nil {
		return err
	}

	hashNodeKey := append(nodeBlobKey[8+common.HashLength:], root[:]...)
	if err := hashNodeDB.Delete(hashNodeKey, pebble.NoSync); err != nil {
		return err
	}

	return nil
}
