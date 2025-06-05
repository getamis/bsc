package pathdb

import (
	"bytes"
	"encoding/hex"
	"io"
	"sync"

	"github.com/spf13/afero"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	onceInitNodeSetDB sync.Once
	nodeSetDB         afero.Fs
	stopGC            bool
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
	onceInitNodeSetDB.Do(func() {
		_ = InitNodeSetDB("", true)
	})

	filename := getNodeSetFilename(root)
	return nodeSetDB.Remove(filename)
}
