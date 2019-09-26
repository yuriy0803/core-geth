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

package badger

import (
	"github.com/dgraph-io/badger"
)

type iterator struct {
	txn  *badger.Txn
	iter *badger.Iterator
}

func newIterator(db *badger.DB, prefix []byte) *iterator {
	t := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	i := t.NewIterator(opts)
	i.Seek(nil)
	return &iterator{
		txn:  t,
		iter: i,
	}
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (i *iterator) Next() bool {
	i.iter.Next()
	return i.iter.Valid()
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error.
func (i *iterator) Error() error {
	// TODO(tzdybal): is there any
	return nil
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (i *iterator) Key() []byte {
	// TODO(tzdybal): check if it's possible to use just Key()
	return i.iter.Item().KeyCopy(nil)
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (i *iterator) Value() []byte {
	value, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		return nil
	}
	return value
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (i *iterator) Release() {
	i.iter.Close()
	i.txn.Discard()
}
