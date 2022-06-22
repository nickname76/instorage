package instorage

import (
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/nickname76/repeater"
)

// Transaction session used by NamespaceSingle and NamespaceMultiple
type Txn struct {
	badgertxn *badger.Txn
}

// Database api object
type DB[TxnAPIT any] struct {
	badgerdb       *badger.DB
	stopGCRepeater func()
	txnAPIBuilder  func(txn Txn) TxnAPIT
}

// Opens database from dbpath and stores txnAPIBuilder for building TxnAPI in View and Update methods of DB
func Open[TxnAPIT any](dbpath string, txnAPIBuilder func(txn Txn) TxnAPIT) (*DB[TxnAPIT], error) {
	if txnAPIBuilder == nil {
		panic("txnAPIBuilder must not be nil")
	}

	badgerdb, err := badger.Open(badger.DefaultOptions(dbpath).WithLoggingLevel(badger.ERROR))
	if err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}

	badgerdb.RunValueLogGC(0.1)

	err = badgerdb.Flatten(16)
	if err != nil {
		return nil, fmt.Errorf("Open: %w", err)
	}

	stopGCRepeater := repeater.StartRepeater(time.Minute, func() {
		badgerdb.RunValueLogGC(0.5)
	})

	return &DB[TxnAPIT]{
		badgerdb:       badgerdb,
		stopGCRepeater: stopGCRepeater,
		txnAPIBuilder:  txnAPIBuilder,
	}, nil
}

// Starts read-write transaction with your TxnAPI.
// If error is returned during transaction, all previous operations under this transaction are discarded.
func (db *DB[TxnAPIT]) Update(updater func(txnAPI TxnAPIT) error) error {
	err := db.badgerdb.Update(func(badgertxn *badger.Txn) error {
		txnAPI := db.txnAPIBuilder(Txn{
			badgertxn: badgertxn,
		})
		return updater(txnAPI)
	})
	if err != nil {
		return fmt.Errorf("Update: %w", err)
	}

	return nil
}

// Starts read-only transaction with your TxnAPI.
func (db *DB[TxnAPIT]) View(viewer func(txnAPI TxnAPIT) error) error {
	err := db.badgerdb.View(func(badgertxn *badger.Txn) error {
		txnAPI := db.txnAPIBuilder(Txn{
			badgertxn: badgertxn,
		})
		return viewer(txnAPI)
	})
	if err != nil {
		return fmt.Errorf("View: %w", err)
	}

	return nil
}

// Deletes all data in database
func (db *DB[TxnAPIT]) DropAll() error {
	err := db.badgerdb.DropAll()
	if err != nil {
		return fmt.Errorf("DropAll: %w", err)
	}

	return nil
}

// Deletes data in passed namespace from database
func (db *DB[TxnAPIT]) DropNamespace(name string) error {
	err := db.badgerdb.DropPrefix([]byte(name))
	if err != nil {
		return fmt.Errorf("DropNamespace: %w", err)
	}

	return nil
}

// Writes database backup to w. Consider adding compression before saving.
func (db *DB[TxnAPIT]) Backup(w io.Writer) error {
	_, err := db.badgerdb.Backup(w, 0)
	if err != nil {
		return fmt.Errorf("Backup: %w", err)
	}

	return nil
}

// Replaces database storage with backup. Should be called when not running any other transactions.
func (db *DB[TxnAPIT]) LoadBackup(r io.Reader) error {
	err := db.badgerdb.DropAll()
	if err != nil {
		return fmt.Errorf("LoadBackup: %w", err)
	}

	err = db.badgerdb.Load(r, 64)
	if err != nil {
		return fmt.Errorf("LoadBackup: %w", err)
	}

	err = db.badgerdb.Flatten(16)
	if err != nil {
		return fmt.Errorf("LoadBackup: %w", err)
	}

	return nil
}

// Waits all pending transactions and closes database. You must call it to ensure that all pending updates are written to disk.
func (db *DB[TxnAPIT]) Close() error {
	db.stopGCRepeater()

	err := db.badgerdb.Close()
	if err != nil {
		return fmt.Errorf("Close: %w", err)
	}

	return nil
}
