package instorage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

// Basic key-value pair for database
type NamespaceSingle[ValueT any] struct {
	txn  Txn
	name string
}

// Creates api for storing single key-value pair with specified name. Do not use
// pointer as a type for ValueT. Name must not be empty.
func NewNamespaceSingle[ValueT any](txn Txn, name string) *NamespaceSingle[ValueT] {
	if name == "" {
		panic("name must not be empty")
	}
	if strings.ContainsRune(name, '\x00') {
		panic("name must not contain \\x00 symbol")
	}
	return &NamespaceSingle[ValueT]{
		txn:  txn,
		name: name,
	}
}

// Sets new value
func (nss *NamespaceSingle[ValueT]) Set(value ValueT) error {
	valueb, err := encodeGob(value)
	if err != nil {
		return fmt.Errorf("Set `%v`: %w", nss.name, err)
	}
	err = nss.txn.badgertxn.Set([]byte(nss.name), valueb)
	if err != nil {
		return fmt.Errorf("Set `%v`: %w", nss.name, err)
	}

	return nil
}

// Returns saved value. If no value stored at the moment, returns default value
// for specified type in NewNamespaceSingle
func (nss *NamespaceSingle[ValueT]) Get() (value ValueT, err error) {
	item, err := nss.txn.badgertxn.Get([]byte(nss.name))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return value, nil
		}

		return value, fmt.Errorf("Get `%v`: %w", nss.name, err)
	}

	var valuePtr *ValueT
	err = item.Value(func(valueb []byte) error {
		var err error
		valuePtr, err = decodeGob[ValueT](valueb)
		return err
	})
	if err != nil {
		return value, fmt.Errorf("Get `%v`: %w", nss.name, err)
	}

	return *valuePtr, nil
}

// Delete key-value pair from database. No error is returned if this key-value
// pair does not exist.
func (nss *NamespaceSingle[ValueT]) Delete() (err error) {
	err = nss.txn.badgertxn.Delete([]byte(nss.name))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}

		return fmt.Errorf("Delete `%v`: %w", nss.name, err)
	}

	return nil
}
