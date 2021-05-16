package dedup

import (
	"fmt"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"math/rand"
	"testing"
	"time"
)

func TestNewDedupGroups(t *testing.T) {
	input := []struct {
		blocks []*metadata.Meta
	}{
		{
			blocks: []*metadata.Meta{
				mockMeta("s0", "r0", 0, 100, 200),
				mockMeta("s0", "r1", 0, 100, 200),
			},
		},
		{
			blocks: []*metadata.Meta{
				mockMeta("s0", "r0", 0, 200, 300),
				mockMeta("s0", "r1", 0, 100, 200),
				mockMeta("s0", "r1", 0, 200, 300),
			},
		},
		{
			blocks: []*metadata.Meta{
				mockMeta("s0", "r0", 0, 100, 200),
				mockMeta("s0", "r0", 0, 200, 300),
				mockMeta("s0", "r0", 0, 300, 400),
				mockMeta("s0", "r1", 0, 200, 400),
			},
		},
		{
			blocks: []*metadata.Meta{
				mockMeta("s0", "r0", 0, 100, 300),
				mockMeta("s0", "r0", 0, 300, 400),
				mockMeta("s0", "r1", 0, 200, 400),
			},
		},
	}

	expected := []struct {
		length    int
		ranges    []*tsdb.TimeRange
		blockNums []int
	}{
		{
			length: 1,
			ranges: []*tsdb.TimeRange{
				{Min: 100, Max: 200},
			},
			blockNums: []int{2},
		},
		{
			length: 2,
			ranges: []*tsdb.TimeRange{
				{Min: 100, Max: 200},
				{Min: 200, Max: 300},
			},
			blockNums: []int{1, 2},
		},
		{
			length: 2,
			ranges: []*tsdb.TimeRange{
				{Min: 100, Max: 200},
				{Min: 200, Max: 400},
			},
			blockNums: []int{1, 3},
		},
		{
			length: 2,
			ranges: []*tsdb.TimeRange{
				{Min: 100, Max: 300},
				{Min: 300, Max: 400},
			},
			blockNums: []int{2, 2},
		},
	}

	for _, v := range input {
		groups := newDedupGroups(v.blocks)
		fmt.Println(groups)
		_ = expected
	}
}

func mockMeta(shard, replica string, resolution, minTime, maxTime int64) *metadata.Meta {
	meta := &metadata.Meta{}
	meta.Version = metadata.ThanosVersion1
	meta.BlockMeta = tsdb.BlockMeta{
		ULID:    ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano()))),
		MinTime: minTime,
		MaxTime: maxTime,
	}
	labels := make(map[string]string)
	labels["shard"] = shard
	labels["replica"] = replica
	meta.Thanos = metadata.Thanos{
		Labels: labels,
		Downsample: metadata.ThanosDownsample{
			Resolution: resolution,
		},
		Source: metadata.TestSource,
	}
	return meta
}
