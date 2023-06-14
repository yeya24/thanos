package store

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

type PostingsFetcher struct {
	r                          *bucketIndexReader
	bytesLimiter               BytesLimiter
	lazyExpandedPostingEnabled bool
}

func fetchLazyExpandedPostings(
	ctx context.Context,
	keys []labels.Label,
	postingGroups []*postingGroup,
	r *bucketIndexReader,
	bytesLimiter BytesLimiter,
	addAllPostings bool,
	lazyExpandedPostingEnabled bool,
) (*lazyExpandedPostings, error) {
	var matchers []*labels.Matcher
	keysToFetch := keys
	// If all postings added, we always download all postings and then intersect.
	if lazyExpandedPostingEnabled && !addAllPostings && len(postingGroups) > 1 {
		var (
			totalCardinality int64
		)
		minCardinality := int64(math.MaxInt64)
		minGroupIdx := -1
		// If the lowest cardinality PG is not equal, we cannot pick it and fetch series.
		// Because those series won't match at all. We need to at least keep a group with add keys.
		for i, pg := range postingGroups {
			vals := pg.addKeys
			if len(pg.removeKeys) > 0 {
				vals = pg.removeKeys
			}
			rngs, err := r.block.indexHeaderReader.PostingsOffsets(pg.matcher.Name, vals...)
			if err != nil {

			}
			for _, r := range rngs {
				pg.cardinality += (r.End - r.Start - 4) / 4
			}
			totalCardinality += pg.cardinality
			if len(pg.addKeys) > 0 && pg.cardinality < minCardinality {
				minCardinality = pg.cardinality
				minGroupIdx = i
			}
		}

		if totalCardinality*4 > (4+int64(r.block.estimatedMaxSeriesSize))*minCardinality {
			for i, pg := range postingGroups {
				if i == minGroupIdx {
					keysToFetch = make([]labels.Label, 0)
					for _, k := range pg.addKeys {
						keysToFetch = append(keysToFetch, labels.Label{Name: pg.matcher.Name, Value: k})
					}
					for _, k := range pg.removeKeys {
						keysToFetch = append(keysToFetch, labels.Label{Name: pg.matcher.Name, Value: k})
					}
					continue
				}
				matchers = append(matchers, pg.matcher)
			}
			postingGroups = []*postingGroup{postingGroups[minGroupIdx]}
		}
	}

	fetchedPostings, closeFns, err := r.fetchPostings(ctx, keysToFetch, bytesLimiter)
	defer func() {
		for _, closeFn := range closeFns {
			closeFn()
		}
	}()
	if err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	// Get "add" and "remove" postings from groups. We iterate over postingGroups and their keys
	// again, and this is exactly the same order as before (when building the groups), so we can simply
	// use one incrementing index to fetch postings from returned slice.
	postingIndex := 0

	var groupAdds, groupRemovals []index.Postings
	for _, g := range postingGroups {
		// We cannot add empty set to groupAdds, since they are intersected.
		if len(g.addKeys) > 0 {
			toMerge := make([]index.Postings, 0, len(g.addKeys))
			for _, l := range g.addKeys {
				toMerge = append(toMerge, checkNilPosting(g.matcher.Name, l, fetchedPostings[postingIndex]))
				postingIndex++
			}

			groupAdds = append(groupAdds, index.Merge(toMerge...))
		}

		for _, l := range g.removeKeys {
			groupRemovals = append(groupRemovals, checkNilPosting(g.matcher.Name, l, fetchedPostings[postingIndex]))
			postingIndex++
		}
	}

	result := index.Without(index.Intersect(groupAdds...), index.Merge(groupRemovals...))

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	ps, err := index.ExpandPostings(result)
	if err != nil {
		return nil, errors.Wrap(err, "expand")
	}
	return &lazyExpandedPostings{ps: ps, matchers: matchers}, nil
}

type lazyExpandedPostings struct {
	ps       []storage.SeriesRef
	matchers []*labels.Matcher
}

func newLazyExpandedPostings(ps []storage.SeriesRef, matchers ...*labels.Matcher) *lazyExpandedPostings {
	return &lazyExpandedPostings{
		ps:       ps,
		matchers: matchers,
	}
}
