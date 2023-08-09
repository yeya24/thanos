package storecache

import (
	"context"
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/oklog/ulid"
)

type ChunkCache interface {
	StoreChunks(blockID ulid.ULID, refs []uint64, values [][]byte)
	FetchChunks(ctx context.Context, blockID ulid.ULID, refs []uint64) (hits map[uint64][]byte, misses []uint64)
}

type BadgerChunkCache struct {
	db *badger.DB
}

func NewBadgerChunkCache(dir string) (*BadgerChunkCache, error) {
	var op badger.Options
	if len(dir) > 0 {
		op = badger.DefaultOptions("").WithDir(dir).WithCompression(options.None)
	} else {
		op = badger.DefaultOptions("").WithInMemory(true).WithCompression(options.None)
	}
	db, err := badger.Open(op)
	if err != nil {
		return nil, err
	}
	return &BadgerChunkCache{db: db}, nil
}

func (c *BadgerChunkCache) StoreChunks(blockID ulid.ULID, refs []uint64, values [][]byte) {
	c.db.Update(func(txn *badger.Txn) error {
		for i, ref := range refs {
			buf := make([]byte, 16+binary.MaxVarintLen64)
			copy(buf, blockID[:])
			binary.PutUvarint(buf[16:], ref)
			txn.Set(buf, values[i])
		}
		return nil
	})
}

func (c *BadgerChunkCache) FetchChunks(ctx context.Context, blockID ulid.ULID, refs []uint64) (map[uint64][]byte, []uint64) {
	hits := make(map[uint64][]byte)
	misses := make([]uint64, 0)
	c.db.View(func(txn *badger.Txn) error {
		for _, ref := range refs {
			if err := ctx.Err(); err != nil {
				return err
			}
			buf := make([]byte, 16+binary.MaxVarintLen64)
			copy(buf, blockID[:])
			binary.PutUvarint(buf[16:], ref)
			item, err := txn.Get(buf)
			if err != nil {
				misses = append(misses, ref)
			} else {
				val, err := item.ValueCopy(nil)
				if err != nil {
					misses = append(misses, ref)
				} else {
					hits[ref] = val
				}
			}
		}
		return nil
	})
	return hits, misses
}
