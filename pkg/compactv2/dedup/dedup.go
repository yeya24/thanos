package dedup

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compactv2"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type dedupGroup struct {
	tr    *tsdb.TimeRange
	metas []*metadata.Meta
}

func newDedupGroups(metas []*metadata.Meta) []*dedupGroup {
	// Prefer to use larger time window to group metas, best effort to not break the compacted metas
	// If two metas with same duration, prefer to handle the one with smaller minTime firstly
	sort.Slice(metas, func(i, j int) bool {
		d1 := metas[i].MaxTime - metas[i].MinTime
		d2 := metas[j].MaxTime - metas[j].MinTime
		if d1 == d2 {
			return metas[i].MinTime < metas[j].MinTime
		}
		return d1 > d2
	})

	groups := make([]*dedupGroup, 0)
	covered := make([]*tsdb.TimeRange, 0)
	for _, m := range metas {
		tw := getUncoveredTimeWindow(covered, m)
		if tw == nil {
			continue
		}
		if dg := getDedupGroup(metas, tw); dg != nil {
			groups = append(groups, dg)
		}

		covered = append(covered, tw)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].tr.Min < groups[j].tr.Min
	})
	return groups
}

func (dg *dedupGroup) ids() []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(dg.metas))
	for _, meta := range dg.metas {
		ids = append(ids, meta.ULID)
	}
	return ids
}

func (dg *dedupGroup) numSeries() uint64 {
	var res uint64
	for _, meta := range dg.metas {
		res += meta.Stats.NumSeries
	}
	return res
}

func getDedupGroup(metas []*metadata.Meta, tr *tsdb.TimeRange) *dedupGroup {
	target := make([]*metadata.Meta, 0)
	for _, m := range metas {
		if m.MaxTime <= tr.Min || m.MinTime >= tr.Max {
			continue
		}
		target = append(target, m)
	}
	// It is unnecessary to deduplicate when the group only has one block.
	if len(target) < 2 {
		return nil
	}
	return &dedupGroup{tr: tr, metas: target}
}

func getUncoveredTimeWindow(covered []*tsdb.TimeRange, b *metadata.Meta) *tsdb.TimeRange {
	minTime := b.MinTime
	maxTime := b.MaxTime
	for _, v := range covered {
		if minTime >= v.Min && minTime < v.Max {
			minTime = v.Max
		}
		if maxTime > v.Min && maxTime <= v.Max {
			maxTime = v.Min
		}
		if minTime >= maxTime {
			return nil
		}
	}
	return &tsdb.TimeRange{Min: minTime, Max: maxTime}
}

type deduper struct {
	bkt      objstore.InstrumentedBucket
	dedupDir string

	grouper compact.Grouper
	syncer  *compact.Syncer
	logger  log.Logger

	chunkPool chunkenc.Pool
	compact   *compactv2.Compactor
}

func RunDedup(
	g *run.Group,
	logger log.Logger,
	bkt objstore.InstrumentedBucket,
	dedupDir string,
	grouper compact.Grouper,
	syncer *compact.Syncer,
	noCompactMarkerFilter *compact.GatherNoCompactionMarkFilter,
	hashFunc metadata.HashFunc,
) error {
	level.Info(logger).Log("msg", "syncing metas metadata")
	if err := syncer.SyncMetas(context.Background()); err != nil {
		return errors.Wrap(err, "sync metas")
	}

	level.Info(logger).Log("msg", "synced metas done")

	metas := syncer.Metas()
	groups, err := grouper.Groups(metas)
	if err != nil {
		return err
	}

	planner := compact.NewPlanner(logger, []int64{int64(2 * time.Hour), int64(8 * time.Hour)}, noCompactMarkerFilter)

	chunkPool := chunkenc.NewPool()
	deduper := &deduper{
		dedupDir:  dedupDir,
		bkt:       bkt,
		logger:    logger,
		chunkPool: chunkPool,
		compact:   compactv2.New(dedupDir, logger, nil, chunkPool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		for _, group := range groups {
			toCompact, err := planner.Plan(ctx, group.MetasByMeanTime())
			if err != nil {
				return err
			}
			if len(toCompact) == 0 {
				continue
			}
			ids := group.IDs()
			if len(ids) < 2 {
				continue
			}
			groupMetas := make([]*metadata.Meta, 0, len(ids))
			for _, id := range ids {
				groupMetas = append(groupMetas, metas[id])
			}
			blocksToDelete := make([]ulid.ULID, 0)
			for _, dedupGroup := range newDedupGroups(groupMetas) {
				readers, err := deduper.prepare(ctx, dedupGroup)
				if err != nil {
					return err
				}
				if len(readers) < 2 {
					continue
				}

				newID := ulid.MustNew(ulid.Now(), rand.Reader)
				if err := os.MkdirAll(filepath.Join(dedupDir, newID.String()), os.ModePerm); err != nil {
					return err
				}
				d, err := block.NewDiskWriter(ctx, logger, filepath.Join(dedupDir, newID.String()))
				if err != nil {
					return err
				}

				p := compactv2.NewProgressLogger(logger, int(dedupGroup.numSeries()))
				if err := deduper.compact.WriteSeries(ctx, readers, d, p, compactv2.NewDedupChunkSeriesMerger(dedupGroup.tr)); err != nil {
					return err
				}

				meta := deduper.updateMetadata(dedupGroup, newID)
				meta.Stats, err = d.Flush()
				if err != nil {
					return err
				}

				if err := meta.WriteToDir(logger, filepath.Join(dedupDir, newID.String())); err != nil {
					return err
				}

				level.Info(logger).Log("msg", "uploading new block", "id", newID)
				if err := block.Upload(ctx, logger, bkt, filepath.Join(dedupDir, newID.String()), hashFunc); err != nil {
					return errors.Wrap(err, "upload")
				}
				level.Info(logger).Log("msg", "uploaded", "id", newID)

				blocksToDelete = append(blocksToDelete, dedupGroup.ids()...)
			}

			// Mark for deletion the blocks we just compacted from the group and bucket so they do not get included
			// into the next planning cycle.
			// Eventually the block we just uploaded should get synced into the group again (including sync-delay).

			for _, id := range blocksToDelete {
				if err := group.DeleteBlock(id, filepath.Join(dedupDir, id.String())); err != nil {
					return err
				}
			}
		}
		return nil
	}, func(err error) {
		cancel()
	})
	return nil
}

func (d *deduper) prepare(ctx context.Context, dg *dedupGroup) ([]block.Reader, error) {
	readers := make([]block.Reader, 0, len(dg.metas))
	for _, id := range dg.ids() {
		level.Info(d.logger).Log("msg", "downloading block", "source", id)
		if err := block.Download(ctx, d.logger, d.bkt, id, filepath.Join(d.dedupDir, id.String())); err != nil {
			return nil, errors.Wrapf(err, "download %v", id)
		}

		b, err := tsdb.OpenBlock(d.logger, filepath.Join(d.dedupDir, id.String()), d.chunkPool)
		if err != nil {
			return nil, errors.Wrapf(err, "open block %v", id)
		}
		readers = append(readers, b)
	}
	return readers, nil
}

func (d *deduper) updateMetadata(dg *dedupGroup, newID ulid.ULID) *metadata.Meta {
	baseMeta := dg.metas[0]
	oldID := baseMeta.ULID
	baseMeta.MinTime = dg.tr.Min
	baseMeta.MaxTime = dg.tr.Max
	baseMeta.ULID = newID

	newSources := make([]ulid.ULID, 0, len(baseMeta.Compaction.Sources))
	var hasOldID bool
	for _, source := range baseMeta.Compaction.Sources {
		if source == oldID {
			hasOldID = true
			continue
		}
		newSources = append(newSources, source)
	}
	if hasOldID {
		newSources = append(newSources, newID)
	}
	baseMeta.Compaction.Sources = newSources
	return baseMeta
}
