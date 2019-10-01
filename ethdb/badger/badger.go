// Copyright 2019 Ethereum Classic Labs Core
// This file is part of the multi-geth library.
//
// The multi-geth library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The multi-geth library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the multi-geth library. If not, see <http://www.gnu.org/licenses/>.

// Package badger implements the key-value database layer based on https://github.com/dgraph-io/badger
package badger

import (
	"os"

	"github.com/dgraph-io/badger"
	"github.com/ethereum/go-ethereum/ethdb"
)

type Database struct {
	db *badger.DB
}

func New(directory string) (*Database, error) {
	os.MkdirAll(directory, 0755)
	options := badger.DefaultOptions(directory)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	return &Database{db}, nil
}

// Has retrieves if a key is present in the key-value data store.
func (d *Database) Has(key []byte) (bool, error) {
	found := false
	err := d.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			found = true
		}
		return err
	})
	return found, err
}

// Get retrieves the given key if it's present in the key-value data store.
func (d *Database) Get(key []byte) ([]byte, error) {
	var value []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Put inserts the given value into the key-value data store.
func (d *Database) Put(key []byte, value []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete removes the key from the key-value data store.
func (d *Database) Delete(key []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// NewBatch creates a write-only database that buffers changes to its host db
// until a final write is called.
func (d *Database) NewBatch() ethdb.Batch {
	return newBatch(d.db)
}

// NewIterator creates a binary-alphabetical iterator over the entire keyspace
// contained within the key-value database.
func (d *Database) NewIterator() ethdb.Iterator {
	return newIterator(d.db, nil)
}

// NewIteratorWithStart creates a binary-alphabetical iterator over a subset of
// database content starting at a particular initial key (or after, if it does
// not exist).
func (d *Database) NewIteratorWithStart(start []byte) ethdb.Iterator {
	iter := newIterator(d.db, nil)
	iter.iter.Seek(start)
	return iter
}

// NewIteratorWithPrefix creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix.
func (d *Database) NewIteratorWithPrefix(prefix []byte) ethdb.Iterator {
	return newIterator(d.db, prefix)
}

// Stat returns a particular internal stat of the database.
func (d *Database) Stat(property string) (string, error) {
	// TODO(tzdybal)
	return "", nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (d *Database) Compact(start []byte, limit []byte) error {
	return d.db.RunValueLogGC(0.5)
}

func (d *Database) Close() error {
	return d.db.Close()
}
