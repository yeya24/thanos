// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerDownsample(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.Downsample
	cmd := app.Command(comp.String(), "continuously downsamples blocks in an object store bucket")

	httpAddr, httpGracePeriod := regHTTPFlags(cmd)

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process downsamplings.").
		Default("./data").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runDownsample(g, logger, reg, *httpAddr, time.Duration(*httpGracePeriod), *dataDir, objStoreConfig, comp)
	}
}

type DownsampleMetrics struct {
	downsamples        *prometheus.CounterVec
	downsampleFailures *prometheus.CounterVec
}

func newDownsampleMetrics(reg *promauto.Factory) *DownsampleMetrics {
	m := new(DownsampleMetrics)

	m.downsamples = reg.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_total",
		Help: "Total number of downsampling attempts.",
	}, []string{"group"})
	m.downsampleFailures = reg.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_failures_total",
		Help: "Total number of failed downsampling attempts.",
	}, []string{"group"})

	return m
}

func runDownsample(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	dataDir string,
	objStoreConfig *extflag.PathOrContent,
	comp component.Component,
) error {
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Downsample.String())
	if err != nil {
		return err
	}

	metaFetcher, err := block.NewMetaFetcher(logger, 32, bkt, "", extprom.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return errors.Wrap(err, "create meta fetcher")
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg)),
	)

	metrics := newDownsampleMetrics()
	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			statusProber.Ready()

			level.Info(logger).Log("msg", "start first pass of downsampling")

			if err := downsampleBucket(ctx, logger, metrics, bkt, metaFetcher, dataDir); err != nil {
				return errors.Wrap(err, "downsampling failed")
			}

			level.Info(logger).Log("msg", "start second pass of downsampling")

			if err := downsampleBucket(ctx, logger, metrics, bkt, metaFetcher, dataDir); err != nil {
				return errors.Wrap(err, "downsampling failed")
			}

			return nil
		}, func(error) {
			cancel()
		})
	}

	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	level.Info(logger).Log("msg", "starting downsample node")
	return nil
}

func downsampleBucket(
	ctx context.Context,
	logger log.Logger,
	metrics *DownsampleMetrics,
	bkt objstore.Bucket,
	fetcher block.MetadataFetcher,
	dir string,
) error {
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrap(err, "clean working directory")
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return errors.Wrap(err, "create dir")
	}

	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			level.Error(logger).Log("msg", "failed to remove downsample cache directory", "path", dir, "err", err)
		}
	}()

	metas, _, err := fetcher.Fetch(ctx)
	if err != nil {
		return errors.Wrap(err, "downsampling meta fetch")
	}

	// mapping from a hash over all source IDs to blocks. We don't need to downsample a block
	// if a downsampled version with the same hash already exists.
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case downsample.ResLevel0:
			continue
		case downsample.ResLevel1:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case downsample.ResLevel2:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
		default:
			return errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case downsample.ResLevel0:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources5m[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < downsample.DownsampleRange0 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, downsample.ResLevel1); err != nil {
				metrics.downsampleFailures.WithLabelValues(compact.GroupKey(m.Thanos)).Inc()
				return errors.Wrap(err, "downsampling to 5 min")
			}
			metrics.downsamples.WithLabelValues(compact.GroupKey(m.Thanos)).Inc()

		case downsample.ResLevel1:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources1h[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < downsample.DownsampleRange1 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, downsample.ResLevel2); err != nil {
				metrics.downsampleFailures.WithLabelValues(compact.GroupKey(m.Thanos))
				return errors.Wrap(err, "downsampling to 60 min")
			}
			metrics.downsamples.WithLabelValues(compact.GroupKey(m.Thanos))
		}
	}
	return nil
}

func processDownsampling(ctx context.Context, logger log.Logger, bkt objstore.Bucket, m *metadata.Meta, dir string, resolution int64) error {
	begin := time.Now()
	bdir := filepath.Join(dir, m.ULID.String())

	err := block.Download(ctx, logger, bkt, m.ULID, bdir)
	if err != nil {
		return errors.Wrapf(err, "download block %s", m.ULID)
	}
	level.Info(logger).Log("msg", "downloaded block", "id", m.ULID, "duration", time.Since(begin))

	if err := block.VerifyIndex(logger, filepath.Join(bdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil {
		return errors.Wrap(err, "input block index not valid")
	}

	begin = time.Now()

	var pool chunkenc.Pool
	if m.Thanos.Downsample.Resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = downsample.NewPool()
	}

	b, err := tsdb.OpenBlock(logger, bdir, pool)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	defer runutil.CloseWithLogOnErr(log.With(logger, "outcome", "potential left mmap file handlers left"), b, "tsdb reader")

	id, err := downsample.Downsample(logger, m, b, dir, resolution)
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())

	level.Info(logger).Log("msg", "downsampled block",
		"from", m.ULID, "to", id, "duration", time.Since(begin))

	if err := block.VerifyIndex(logger, filepath.Join(resdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil {
		return errors.Wrap(err, "output block index not valid")
	}

	begin = time.Now()

	err = block.Upload(ctx, logger, bkt, resdir)
	if err != nil {
		return errors.Wrapf(err, "upload downsampled block %s", id)
	}

	level.Info(logger).Log("msg", "uploaded block", "id", id, "duration", time.Since(begin))

	// It is not harmful if these fails.
	if err := os.RemoveAll(bdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "dir", bdir, "err", err)
	}
	if err := os.RemoveAll(resdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "resdir", bdir, "err", err)
	}

	return nil
}
