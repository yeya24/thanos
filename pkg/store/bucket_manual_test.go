/// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.
package store

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// Versioned here: https://gist.github.com/bwplotka/cbcbbcd1802181b7785da11dcc0f5cfd

var (
	testCases = []struct {
		bktConfig string
		cases     []*storetestutil.SeriesCase
	}{
		{
			bktConfig: `/home/yeya24/bucket.yaml`,
			cases: []*storetestutil.SeriesCase{
				{
					Name: "alerts2w/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "alerts",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 2191932),
				},
				{
					// Simulate instant query (still 2k results).
					Name: "alerts15s/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1605139200000 - int64((15*time.Second)/time.Millisecond),
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "alerts",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 21439),
				},
				{
					Name: "subssyncs2w/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "subscription_sync_total",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 306010),
				},
				{
					Name: "subs2w/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "subscription_labels",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 1122503),
				},
				{
					Name: "subs2w1account/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "subscription_labels",
								Type: storepb.LabelMatcher_EQ,
							},
							{
								Name: "ebs_account", Value: "6274079",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 406655), // 0.9s
				},
				{
					Name: "subs15m1account/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "subscription_labels",
								Type: storepb.LabelMatcher_EQ,
							},
							{
								Name: "ebs_account", Value: "6274079",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 406655), // 0.5-0.6s
				},
				{
					// Simulate instant query.
					Name: "subs15s/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1605139200000 - int64((15*time.Second)/time.Millisecond),
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "subscription_labels",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 0),
				},
			},
		},
		{
			// 200 MB index header. 166521739814ns (~2m) to create index header!
			bktConfig: "/home/yeya24/bucket.yaml",
			cases: []*storetestutil.SeriesCase{
				{
					Name: "alerts/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "alerts",
								Type: storepb.LabelMatcher_EQ,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 224002),
				},
				{
					Name: "subs/01EPXBGA0413QZMTF4XQ0M4Q3E",
					Req: &storepb.SeriesRequest{
						MinTime: 1603929600000,
						MaxTime: 1605139200000,
						Matchers: []storepb.LabelMatcher{
							{
								Name: "__name__", Value: "subscription_sync_total",
								Type: storepb.LabelMatcher_EQ,
							},
							{
								Name: "installed", Value: ".*hyperconverged.*",
								Type: storepb.LabelMatcher_RE,
							},
						},
						SkipChunks: true,
					},
					ExpectedSeries: make([]*storepb.Series, 224002),
				},
			},
		},
	}
)

func TestTelemeterRealData_Series(t *testing.T) {
	seriesBlock(testutil.NewTB(t))
}

func BenchmarkTelemeterRealData_Series(b *testing.B) {
	seriesBlock(testutil.NewTB(b))
}

func seriesBlock(t testutil.TB) {
	tmpDir, err := ioutil.TempDir("", "testorbench-manual-bucketseries")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	logger := log.NewNopLogger()
	if !t.IsBenchmark() {
		logger = log.NewLogfmtLogger(os.Stderr)
	}

	// Hardcoded choice.
	bktConfig := testCases[0].bktConfig
	cases := testCases[0].cases
	//cases := testCases[0].cases[4:8]

	//bktConfig := testCases[1].bktConfig
	//cases := testCases[1].cases[1:] // Just alerts for now.

	b, err := ioutil.ReadFile(bktConfig)
	testutil.Ok(t, err)

	bkt, err := client.NewBucket(logger, b, nil, "yolo")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	f, err := block.NewMetaFetcher(logger, 32, bkt, "/tmp/test", nil, nil, nil)
	testutil.Ok(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{
		MaxSize:     model.Bytes(1024*1024*250),
		MaxItemSize: storecache.DefaultInMemoryIndexCacheConfig.MaxItemSize,
	})

	st, err := NewBucketStore(
		logger,
		nil,
		bkt,
		f,
		tmpDir,
		indexCache,
		nil,
		1024 * 1024 * 1024 * 2,
		NewChunksLimiterFactory(0),
		NewSeriesLimiterFactory(0),
		false,
		1,
		nil,
		false,
		DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, st.SyncBlocks(context.Background()))
	storetestutil.TestServerSeries(t, st, cases...)
}
