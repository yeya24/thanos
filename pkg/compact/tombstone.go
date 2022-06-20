// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tombstone"
)

type TombstoneSyncer struct {
	bkt              objstore.InstrumentedBucket
	dataDir          string
	logger           log.Logger
	sy               *Syncer
	tombstoneFetcher tombstone.Fetcher
	bucketBlocks     map[ulid.ULID]*store.BucketBlock
	indexReaderPool  *indexheader.ReaderPool
	partitioner      store.Partitioner
	bucketMetrics    *store.BucketStoreMetrics
	chunkPool        pool.Bytes
	indexCache       storecache.IndexCache
}

func NewTombstoneSyncer(logger log.Logger, dataDir string, syncer *Syncer, tombstoneFetcher tombstone.Fetcher, reg prometheus.Registerer) (*TombstoneSyncer, error) {
	readerPool := indexheader.NewReaderPool(logger, false, 0, nil)
	chunkPool, err := store.NewDefaultChunkBytesPool(uint64(1024 * 1024 * 1024 * 2))
	if err != nil {
		return nil, err
	}
	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, reg, storecache.InMemoryIndexCacheConfig{
		MaxSize:     model.Bytes(uint64(1024 * 1024 * 1024 * 2)),
		MaxItemSize: storecache.DefaultInMemoryIndexCacheConfig.MaxItemSize,
	})
	return &TombstoneSyncer{
		logger:           logger,
		dataDir:          dataDir,
		sy:               syncer,
		tombstoneFetcher: tombstoneFetcher,
		bucketBlocks:     map[ulid.ULID]*store.BucketBlock{},
		indexReaderPool:  readerPool,
		partitioner:      store.NewGapBasedPartitioner(store.PartitionerMaxGapSize),
		bucketMetrics:    store.NewBucketStoreMetrics(extprom.WrapRegistererWithPrefix("thanos_compact_", reg)),
		chunkPool:        chunkPool,
		indexCache:       indexCache,
	}, nil
}

// SyncTombstones collects block metadata and tombstones and generate
// per-block tombstone caches for each block accordingly.
func (t *TombstoneSyncer) SyncTombstones(ctx context.Context) error {
	if err := t.sy.SyncMetas(ctx); err != nil {
		return err
	}
	metas := t.sy.Metas()
	tombstones, _, err := t.tombstoneFetcher.Fetch(ctx)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		meta := meta
		dir := filepath.Join(t.dataDir, meta.ULID.String())
		for _, tb := range tombstones {
			matchers, ok := tb.MatchMeta(meta)
			if !ok {
				continue
			}

			b, ok := t.bucketBlocks[meta.ULID]
			if !ok {
				indexHeaderReader, err := t.indexReaderPool.NewBinaryReader(
					ctx,
					t.logger,
					t.bkt,
					t.dataDir,
					meta.ULID,
					16,
				)
				if err != nil {
					continue
				}
				b, err = store.NewBucketBlock(ctx, t.logger, t.bucketMetrics, meta, t.bkt, dir, t.indexCache, t.chunkPool, indexHeaderReader, t.partitioner)
				if err != nil {
					return err
				}
			}

			if _, ok := b.TombstoneCache().Get(tb.ULID); ok {
				continue
			}
			memTombstone, err := b.ProcessTombstone(ctx, *matchers, tb.MinTime, tb.MaxTime)
			if err != nil {
				continue
			}
			b.TombstoneCache().Set(tb.ULID, memTombstone)
		}
		if b, ok := t.bucketBlocks[meta.ULID]; ok {
			if err := b.Close(); err != nil {
				continue
			}
		}
	}
	return nil
}

// GarbageCollect garbage collects tombstones based on metadatas.
func (t *TombstoneSyncer) GarbageCollect(ctx context.Context) error {
	if err := t.sy.SyncMetas(ctx); err != nil {
		return err
	}
	metas := t.sy.Metas()
	tombstones, _, err := t.tombstoneFetcher.Fetch(ctx)
	if err != nil {
		return err
	}

	// Remove block that are already deleted or compacted.
	for id := range t.bucketBlocks {
		if _, exists := metas[id]; !exists {
			delete(t.bucketBlocks, id)
		}
	}

OUTER:
	for _, tb := range tombstones {
		for _, meta := range metas {
			if _, ok := tb.MatchMeta(meta); ok {
				continue
			}

			b, ok := t.bucketBlocks[meta.ULID]
			if !ok {
				// Maybe the block is the newly compacted block.
				// We continue next round.
				// TODO: we can also check the compaction sources to see if
				// the source blocks are compacted by tombstones. We can also cleanup
				// the cache after compaction is done.
				continue OUTER
			}

			if memTombstone, ok := b.TombstoneCache().Get(tb.ULID); ok {
				// MemTombstone is not empty, this tombstone still needs to be cleaned up.
				if memTombstone.Total() > 0 {
					continue OUTER
				}
			}
		}
		// The tombstone deletion is done, Remove tombstone from cache.
		for _, block := range t.bucketBlocks {
			block.TombstoneCache().Delete(tb.ULID)
		}
		// Remove the tombstone from the object storage.
		if err := tombstone.RemoveTombstone(ctx, tb.ULID, t.bkt); err != nil {
			return err
		}
	}
	return nil
}

func (t *TombstoneSyncer) getBucketBlock(id ulid.ULID) *store.BucketBlock {
	return t.bucketBlocks[id]
}
