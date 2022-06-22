# Instorage

Simple, easy to use database for faster development of small projects and MVPs in Go. Uses [Badger](https://github.com/dgraph-io/badger) as a storage.

This database uses key-value architecture with namespaces.

This library lets you describe simple database API just for your project using your own types for keys and values (see exapmple below). Database is portable, and suits best for SSDs.

Documentation: https://pkg.go.dev/github.com/nickname76/instorage

## Example usage

```Go
package main

import (
	"instorage"
	"log"
)

type DataBValue struct {
	A string
	B map[string]string
}

type DBTxnAPI struct {
	DataA *instorage.NamespaceSingle[int64]
	DataB *instorage.NamespaceMultiple[string, DataBValue]
}

func main() {
	db, err := instorage.Open[DBTxnAPI]("./database_storage", func(txn instorage.Txn) DBTxnAPI {
		return DBTxnAPI{
			DataA: instorage.NewNamespaceSingle[int64](txn, "data_a"),
			DataB: instorage.NewNamespaceMultiple[string, DataBValue](txn, "data_b"),
		}
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	err = db.Update(func(txnAPI DBTxnAPI) error {
		valueA, err := txnAPI.DataA.Get()
		if err != nil {
			return err
		}

		log.Println(valueA)

		err = txnAPI.DataA.Set(99999)
		if err != nil {
			return err
		}

		valueA, err = txnAPI.DataA.Get()
		if err != nil {
			return err
		}

		log.Println(valueA)

		err = txnAPI.DataB.Set("1234", DataBValue{
			A: "5678",
			B: map[string]string{
				"test_key": "test_value",
			},
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	err = db.View(func(txnAPI DBTxnAPI) error {
		valueB, _, err := txnAPI.DataB.Get("1234")
		if err != nil {
			return err
		}

		log.Println(valueB)

		_, ok, err := txnAPI.DataB.Get("not_existing_key")
		if err != nil {
			return err
		}

		if !ok {
			log.Println("not_existing_key does not exist")
		}

		valueA, err := txnAPI.DataA.Get()
		if err != nil {
			return err
		}

		log.Println(valueA)

		txnAPI.DataB.Iter(func(key string, value DataBValue) (stop bool, err error) {
			log.Println(key, value)
			return false, nil
		})

		return nil
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

```