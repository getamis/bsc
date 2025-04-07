package blobdb

import (
	"encoding/binary"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	numLevels  = 7
	minHandles = 256
)

var (
	once    sync.Once
	db      *pebble.DB
	cnt     uint64
	nilBlob *Blob
)

func InitDB(name string, inMemory bool) error {
	//Initialize nil blob
	if nilBlob == nil {
		nilBlob = &Blob{Len: 0, idx: 0}
	}

	// Already initialized
	if db != nil {
		return nil
	}

	// Use in-memory DB if no name is provided
	if name == "" && !inMemory {
		inMemory = true
	}

	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(1024 * 1024 * 1024)),
		MaxOpenFiles: minHandles,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		MaxConcurrentCompactions: runtime.NumCPU,

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		// Levels: []pebble.LevelOptions{
		// 	{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// 	{TargetFileSize: 4 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// 	{TargetFileSize: 8 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// 	{TargetFileSize: 16 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// 	{TargetFileSize: 32 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// 	{TargetFileSize: 64 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// 	{TargetFileSize: 128 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		// },
		Levels: make([]pebble.LevelOptions, numLevels),
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

	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	if inMemory {
		opt.FS = vfs.NewMem()
		name = "" // Empty name for in-memory DB
	} else {
		if err := os.RemoveAll(name); err != nil {
			return err
		}
	}

	var err error
	db, err = pebble.Open(name, opt)
	return err
}

type Blob struct {
	Len int
	idx uint64
}

func (b *Blob) key() []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, b.idx)
	return key
}

func (b *Blob) Blob() []byte {
	if b.Len == 0 {
		return nil
	}

	value, closer, _ := db.Get(b.key())
	ret := make([]byte, len(value))
	copy(ret, value)
	_ = closer.Close()
	return ret
}

func New(blob []byte) *Blob {
	// Initialize the database for testing
	once.Do(func() {
		InitDB("", true)
	})

	if len(blob) == 0 {
		return nilBlob
	}

	// Increase the counter
	idx := atomic.AddUint64(&cnt, 1)
	b := &Blob{Len: len(blob), idx: idx}

	// Insert the node blob into the database
	_ = db.Set(b.key(), blob, pebble.NoSync)

	// Set the finalizer to delete the node blob from the database
	runtime.SetFinalizer(b, func(b *Blob) {
		_ = db.Delete(b.key(), pebble.NoSync)
	})

	return b
}
