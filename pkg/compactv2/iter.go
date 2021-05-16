package compactv2

import (
	"container/heap"
	"math"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/thanos-io/thanos/pkg/query"
)

type dedupChunksIterator struct {
	iterators []chunks.Iterator
	h         chunkIteratorHeap

	err  error
	curr chunks.Meta

	tr *tsdb.TimeRange
}

func newDedupChunksIterator(tr *tsdb.TimeRange, replicas ...storage.ChunkSeries) chunks.Iterator {
	iterators := make([]chunks.Iterator, 0, len(replicas))
	for _, s := range replicas {
		iterators = append(iterators, s.Iterator())
	}
	return &dedupChunksIterator{
		iterators: iterators,
		tr:        tr,
	}
}

type chunkIteratorHeap []chunks.Iterator

func (h chunkIteratorHeap) Len() int      { return len(h) }
func (h chunkIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h chunkIteratorHeap) Less(i, j int) bool {
	at := h[i].At()
	bt := h[j].At()
	if at.MinTime == bt.MinTime {
		return at.MaxTime < bt.MaxTime
	}
	return at.MinTime < bt.MinTime
}

func (h *chunkIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(chunks.Iterator))
}

func (h *chunkIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (d *dedupChunksIterator) At() chunks.Meta {
	return d.curr
}

func (c *dedupChunksIterator) Next() bool {
	if c.h == nil {
		for _, iter := range c.iterators {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}
	}
	if len(c.h) == 0 {
		return false
	}

	iter := heap.Pop(&c.h).(chunks.Iterator)
	c.curr = iter.At()
	if iter.Next() {
		heap.Push(&c.h, iter)
	}

	var (
		overlapping []storage.Series
		oMaxTime    = c.curr.MaxTime
	)

	// Detect overlaps to compact. Be smart about it and deduplicate on the fly if chunks are identical.
	for len(c.h) > 0 {
		// Get the next oldest chunk by min, then max time.
		next := c.h[0].At()
		if next.MinTime > oMaxTime {
			// No overlap with current one.
			break
		}

		// We operate on same series, so labels does not matter here.
		overlapping = append(overlapping, newChunkToSeriesDecoder(nil, next, c.tr))
		if next.MaxTime > oMaxTime {
			oMaxTime = next.MaxTime
		}

		iter := heap.Pop(&c.h).(chunks.Iterator)
		if iter.Next() {
			heap.Push(&c.h, iter)
		}
	}
	if len(overlapping) == 0 {
		return true
	}

	// Add last as it's not yet included in overlap. We operate on same series, so labels does not matter here.
	iter = (&seriesToChunkEncoder{Series: &storage.SeriesEntry{
		Lset: nil,
		SampleIteratorFn: func() chunkenc.Iterator {
			it := newChunkToSeriesDecoder(nil, c.curr, c.tr).Iterator()

			for _, o := range overlapping {
				it = newDedupSamplesIterator(it, o.Iterator())
			}
			return it
		},
	}}).Iterator()
	if !iter.Next() {
		if c.err = iter.Err(); c.err != nil {
			return false
		}
		panic("unexpected seriesToChunkEncoder lack of iterations")
	}
	c.curr = iter.At()
	if iter.Next() {
		heap.Push(&c.h, iter)
	}
	return true
}

func (d *dedupChunksIterator) Err() error {
	return d.err
}

type dedupSamplesIterator struct {
	a, b chunkenc.Iterator

	aok, bok bool

	// TODO(bwplotka): Don't base on LastT, but on detected scrape interval. This will allow us to be more
	// responsive to gaps: https://github.com/thanos-io/thanos/issues/981, let's do it in next PR.
	lastT int64

	penA, penB int64
	useA       bool
}

func newDedupSamplesIterator(a, b chunkenc.Iterator) *dedupSamplesIterator {
	return &dedupSamplesIterator{
		a:     a,
		b:     b,
		lastT: math.MinInt64,
		aok:   a.Next(),
		bok:   b.Next(),
	}
}

func (it *dedupSamplesIterator) Next() bool {
	// Advance both iterators to at least the next highest timestamp plus the potential penalty.
	if it.aok {
		it.aok = it.a.Seek(it.lastT + 1 + it.penA)
	}
	if it.bok {
		it.bok = it.b.Seek(it.lastT + 1 + it.penB)
	}

	// Handle basic cases where one iterator is exhausted before the other.
	if !it.aok {
		it.useA = false
		if it.bok {
			it.lastT, _ = it.b.At()
			it.penB = 0
		}
		return it.bok
	}
	if !it.bok {
		it.useA = true
		it.lastT, _ = it.a.At()
		it.penA = 0
		return true
	}
	// General case where both iterators still have data. We pick the one
	// with the smaller timestamp.
	// The applied penalty potentially already skipped potential samples already
	// that would have resulted in exaggerated sampling frequency.
	ta, _ := it.a.At()
	tb, _ := it.b.At()

	it.useA = ta <= tb

	// For the series we didn't pick, add a penalty twice as high as the delta of the last two
	// samples to the next seek against it.
	// This ensures that we don't pick a sample too close, which would increase the overall
	// sample frequency. It also guards against clock drift and inaccuracies during
	// timestamp assignment.
	// If we don't know a delta yet, we pick 5000 as a constant, which is based on the knowledge
	// that timestamps are in milliseconds and sampling frequencies typically multiple seconds long.
	const initialPenalty = 5000

	if it.useA {
		if it.lastT != math.MinInt64 {
			it.penB = 2 * (ta - it.lastT)
		} else {
			it.penB = initialPenalty
		}
		it.penA = 0
		it.lastT = ta
		return true
	}
	if it.lastT != math.MinInt64 {
		it.penA = 2 * (tb - it.lastT)
	} else {
		it.penA = initialPenalty
	}
	it.penB = 0
	it.lastT = tb
	return true
}

func (it *dedupSamplesIterator) Seek(t int64) bool {
	// Don't use underlying Seek, but iterate over next to not miss gaps.
	for {
		ts, _ := it.At()
		if ts >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
}

func (it *dedupSamplesIterator) At() (int64, float64) {
	if it.useA {
		return it.a.At()
	}
	return it.b.At()
}

func (it *dedupSamplesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}

type seriesToChunkEncoder struct {
	storage.Series
}

// TODO(bwplotka): Currently encoder will just naively build one chunk, without limit. Split it: https://github.com/prometheus/tsdb/issues/670
func (s *seriesToChunkEncoder) Iterator() chunks.Iterator {
	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	if err != nil {
		return errChunksIterator{err: err}
	}
	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)

	seriesIter := s.Series.Iterator()
	for seriesIter.Next() {
		t, v := seriesIter.At()
		app.Append(t, v)

		maxt = t
		if mint == math.MaxInt64 {
			mint = t
		}
	}
	if err := seriesIter.Err(); err != nil {
		return errChunksIterator{err: err}
	}

	return storage.NewListChunkSeriesIterator(chunks.Meta{
		MinTime: mint,
		MaxTime: maxt,
		Chunk:   chk,
	})
}

type errChunksIterator struct {
	err error
}

func (e errChunksIterator) At() chunks.Meta { return chunks.Meta{} }
func (e errChunksIterator) Next() bool      { return false }
func (e errChunksIterator) Err() error      { return e.err }

func newChunkToSeriesDecoder(labels labels.Labels, chk chunks.Meta, tr *tsdb.TimeRange) storage.Series {
	return &storage.SeriesEntry{
		Lset: labels,
		SampleIteratorFn: func() chunkenc.Iterator {
			// TODO(bwplotka): Can we provide any chunkenc buffer?
			return query.NewBoundedSeriesIterator(chk.Chunk.Iterator(nil), tr.Min, tr.Max)
		},
	}
}

func NewDedupChunkSeriesMerger(tr *tsdb.TimeRange) storage.VerticalChunkSeriesMergeFunc {
	return func(series ...storage.ChunkSeries) storage.ChunkSeries {
		if len(series) == 0 {
			return nil
		}
		return &storage.ChunkSeriesEntry{
			Lset: series[0].Labels(),
			ChunkIteratorFn: func() chunks.Iterator {
				return newDedupChunksIterator(tr, series...)
			},
		}
	}
}
