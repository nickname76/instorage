package instorage

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

// Transaction session used by NamespaceSingle and NamespaceMultiple
type Txn struct {
	badgertxn *badger.Txn
}

func encodeGob(data any) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(data)
	if err != nil {
		return nil, fmt.Errorf("encodeGob: %w", err)
	}

	return buf.Bytes(), nil
}

func decodeGob[DataT any](b []byte) (dataPtr *DataT, err error) {
	dataPtr = new(DataT)
	err = gob.NewDecoder(bytes.NewReader(b)).Decode(dataPtr)
	if err != nil {
		return dataPtr, fmt.Errorf("decodeGob: %w", err)
	}

	return dataPtr, nil
}

func addPrefixToKey(prefix []byte, key []byte) []byte {
	return bytes.Join([][]byte{prefix, key}, []byte{0x00})
}

func removePrefixFromKey(prefix []byte, key []byte) []byte {
	return key[len(prefix)+1:]
}
