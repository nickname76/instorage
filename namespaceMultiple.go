package instorage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

// Stores multiple key-value pairs under same namespace
type NamespaceMultiple[KeyT comparable, ValueT any] struct {
	txn  Txn
	name string
}

// Creates api for storing multiple key-value pairs under same namespace. Do not
// use pointers as types for KeyT and ValueT. Name must not be empty.
func NewNamespaceMultiple[KeyT comparable, ValueT any](txn Txn, name string) *NamespaceMultiple[KeyT, ValueT] {
	if name == "" {
		panic("name must not be empty")
	}
	if strings.ContainsRune(name, '\x00') {
		panic("name must not contain \\x00 symbol")
	}
	return &NamespaceMultiple[KeyT, ValueT]{
		txn:  txn,
		name: name,
	}
}

// Sets a new value for a key
func (nsm *NamespaceMultiple[KeyT, ValueT]) Set(key KeyT, value ValueT) error {
	keyb, err := encodeGob(key)
	if err != nil {
		return fmt.Errorf("Set `%v`: %w", nsm.name, err)
	}
	valueb, err := encodeGob(value)
	if err != nil {
		return fmt.Errorf("Set `%v`: %w", nsm.name, err)
	}

	err = nsm.txn.badgertxn.Set(addPrefixToKey([]byte(nsm.name), keyb), valueb)
	if err != nil {
		return fmt.Errorf("Set `%v`: %w", nsm.name, err)
	}

	return nil
}

// Returns value stored under a key. Returns ok == false if key does not exist.
func (nsm *NamespaceMultiple[KeyT, ValueT]) Get(key KeyT) (value ValueT, ok bool, err error) {
	keyb, err := encodeGob(key)
	if err != nil {
		return value, false, fmt.Errorf("Get `%v`: %w", nsm.name, err)
	}

	item, err := nsm.txn.badgertxn.Get(addPrefixToKey([]byte(nsm.name), keyb))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return value, false, nil
		}

		return value, false, fmt.Errorf("Get `%v`: %w", nsm.name, err)
	}

	var valuePtr *ValueT
	err = item.Value(func(valueb []byte) error {
		var err error
		valuePtr, err = decodeGob[ValueT](valueb)
		return err
	})
	if err != nil {
		return value, false, fmt.Errorf("Get `%v`: %w", nsm.name, err)
	}

	return *valuePtr, true, nil
}

// Deletes key-value pair. No error is returned, if passed key does not exist.
func (nsm *NamespaceMultiple[KeyT, ValueT]) Delete(key KeyT) (err error) {
	keyb, err := encodeGob(key)
	if err != nil {
		return fmt.Errorf("Delete `%v`: %w", nsm.name, err)
	}

	err = nsm.txn.badgertxn.Delete(addPrefixToKey([]byte(nsm.name), keyb))
	if err != nil {
		return fmt.Errorf("Delete `%v`: %w", nsm.name, err)
	}

	return nil
}

// Iterates over all key-value pairs in this namespace. If viewer function
// returns stop == true, then iteration stops.
func (nsm *NamespaceMultiple[KeyT, ValueT]) Iter(viewer func(key KeyT, value ValueT) (stop bool, err error)) error {
	it := nsm.txn.badgertxn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := []byte(nsm.name)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()

		k := item.Key()

		var stop bool
		err := item.Value(func(valueb []byte) error {
			keyb := removePrefixFromKey(prefix, k)

			keyPtr, err := decodeGob[KeyT](keyb)
			if err != nil {
				return err
			}
			valuePtr, err := decodeGob[ValueT](valueb)
			if err != nil {
				return err
			}

			stop, err = viewer(*keyPtr, *valuePtr)
			return err
		})
		if err != nil {
			return fmt.Errorf("Iter `%v`: %w", nsm.name, err)
		}

		if stop {
			break
		}
	}

	return nil
}

func (nsm *NamespaceMultiple[KeyT, ValueT]) FindKeyByValue(value ValueT) (key KeyT, ok bool, err error) {
	targetvalueb, err := encodeGob(value)
	if err != nil {
		return key, false, fmt.Errorf("FindKeyByValue `%v`: %w", nsm.name, err)
	}

	targetvaluebStr := string(targetvalueb)

	it := nsm.txn.badgertxn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := []byte(nsm.name)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()

		k := item.Key()

		var stop bool
		err := item.Value(func(valueb []byte) error {
			if string(valueb) != targetvaluebStr {
				return nil
			}

			keyb := removePrefixFromKey(prefix, k)

			keyPtr, err := decodeGob[KeyT](keyb)
			if err != nil {
				return err
			}

			key = *keyPtr
			stop = true

			return nil
		})
		if err != nil {
			return key, false, fmt.Errorf("FindKeyByValue `%v`: %w", nsm.name, err)
		}

		if stop {
			break
		}
	}

	return key, true, nil
}
