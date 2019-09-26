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
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

type batch struct {
	db        *badger.DB
	batch     *badger.WriteBatch
	memBatch  ethdb.Batch // this is not pretty...
	valueSize int
}

func newBatch(db *badger.DB) *batch {
	return &batch{
		db:        db,
		batch:     db.NewWriteBatch(),
		memBatch:  memorydb.New().NewBatch(),
		valueSize: 0,
	}
}

// Put inserts the given value into the key-value data store.
func (b *batch) Put(key []byte, value []byte) error {
	err := b.batch.Set(key, value)
	if err == nil {
		b.memBatch.Put(key, value)
		b.valueSize += len(value)
	}
	return err
}

// Delete removes the key from the key-value data store.
func (b *batch) Delete(key []byte) error {
	err := b.batch.Delete(key)
	if err == nil {
		b.memBatch.Delete(key)
		b.valueSize++
	}
	return err
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.valueSize
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	b.memBatch.Write()
	return b.batch.Flush()
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.batch.Cancel()
	b.batch = b.db.NewWriteBatch()
	b.memBatch.Reset()
	b.valueSize = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	return b.memBatch.Replay(w)
}
