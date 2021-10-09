// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// TODO(yeya24): use upstream code once it is exposed.

package compactv2

import (
	"math"

	"container/heap"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

// chainSampleIterator is responsible to iterate over samples from different iterators of the same time series in timestamps
// order. If one or more samples overlap, one sample from random overlapped ones is kept and all others with the same
// timestamp are dropped. It's optimized for non-overlap cases as well.
type chainSampleIterator struct {
	iterators []chunkenc.Iterator
	h         samplesIteratorHeap

	curr  chunkenc.Iterator
	lastt int64
}

func newChainSampleIterator(iterators []chunkenc.Iterator) chunkenc.Iterator {
	return &chainSampleIterator{
		iterators: iterators,
		h:         nil,
		lastt:     math.MinInt64,
	}
}

func (c *chainSampleIterator) Seek(t int64) bool {
	c.h = samplesIteratorHeap{}
	for _, iter := range c.iterators {
		if iter.Seek(t) {
			heap.Push(&c.h, iter)
		}
	}
	if len(c.h) > 0 {
		c.curr = heap.Pop(&c.h).(chunkenc.Iterator)
		c.lastt, _ = c.curr.At()
		return true
	}
	c.curr = nil
	return false
}

func (c *chainSampleIterator) At() (t int64, v float64) {
	if c.curr == nil {
		panic("chainSampleIterator.At() called before first .Next() or after .Next() returned false.")
	}
	return c.curr.At()
}

func (c *chainSampleIterator) Next() bool {
	if c.h == nil {
		c.h = samplesIteratorHeap{}
		// We call c.curr.Next() as the first thing below.
		// So, we don't call Next() on it here.
		c.curr = c.iterators[0]
		for _, iter := range c.iterators[1:] {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}
	}

	if c.curr == nil {
		return false
	}

	var currt int64
	for {
		if c.curr.Next() {
			currt, _ = c.curr.At()
			if currt == c.lastt {
				// Ignoring sample for the same timestamp.
				continue
			}
			if len(c.h) == 0 {
				// curr is the only iterator remaining,
				// no need to check with the heap.
				break
			}

			// Check current iterator with the top of the heap.
			if nextt, _ := c.h[0].At(); currt < nextt {
				// Current iterator has smaller timestamp than the heap.
				break
			}
			// Current iterator does not hold the smallest timestamp.
			heap.Push(&c.h, c.curr)
		} else if len(c.h) == 0 {
			// No iterator left to iterate.
			c.curr = nil
			return false
		}

		c.curr = heap.Pop(&c.h).(chunkenc.Iterator)
		currt, _ = c.curr.At()
		if currt != c.lastt {
			break
		}
	}

	c.lastt = currt
	return true
}

func (c *chainSampleIterator) Err() error {
	errs := tsdb_errors.NewMulti()
	for _, iter := range c.iterators {
		errs.Add(iter.Err())
	}
	return errs.Err()
}

type samplesIteratorHeap []chunkenc.Iterator

func (h samplesIteratorHeap) Len() int      { return len(h) }
func (h samplesIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h samplesIteratorHeap) Less(i, j int) bool {
	at, _ := h[i].At()
	bt, _ := h[j].At()
	return at < bt
}

func (h *samplesIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(chunkenc.Iterator))
}

func (h *samplesIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
