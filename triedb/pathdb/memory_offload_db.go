package pathdb

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	numLevels  = 7
	minHandles = 524288
)

var (
	onceInitNodeSetDB sync.Once
	nodeSetDB         *pebble.DB
	stopGC            bool
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

func InitNodeSetDB(name string, inMemory bool) error {
	// Already initialized
	if nodeSetDB != nil {
		return nil
	}

	// Initialize the database
	db, err := initDB(name, inMemory)
	if err != nil {
		return err
	}
	nodeSetDB = db
	return nil
}

func disableGCForDB() {
	stopGC = true
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

	return nodeSetDB.Set(root[:], buf.Bytes(), pebble.NoSync)
}

func getNodeSet(root common.Hash) (*nodeSet, error) {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	data, closer, err := nodeSetDB.Get(root[:])
	if err != nil {
		return nil, err
	}
	defer closer.Close()

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

	data, closer, err := nodeSetDB.Get(root[:])
	if err != nil {
		return err
	}
	defer closer.Close()

	_, err = w.Write(data)
	return err
}

func deleteNodeSet(root common.Hash) error {
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	if err := nodeSetDB.Delete(root[:], pebble.NoSync); err != nil {
		return err
	}
	return nil
}
