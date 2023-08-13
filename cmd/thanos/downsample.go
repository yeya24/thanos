// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
)

func RunDownsample(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	httpBindAddr string,
	httpTLSConfig string,
	httpGracePeriod time.Duration,
	dataDir string,
	waitInterval time.Duration,
	downsampleConcurrency int,
	blockFilesConcurrency int,
	objStoreConfig *extflag.PathOrContent,
	comp component.Component,
	hashFunc metadata.HashFunc,
) error {
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, component.Downsample.String())
	if err != nil {
		return err
	}
	insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

	// While fetching blocks, filter out blocks that were marked for no downsample.
	metaFetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, insBkt, "", extprom.WrapRegistererWithPrefix("thanos_", reg), []block.MetadataFilter{
		block.NewDeduplicateFilter(block.FetcherConcurrency),
		downsample.NewGatherNoDownsampleMarkFilter(logger, insBkt),
	})
	if err != nil {
		return errors.Wrap(err, "create meta fetcher")
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")
		}
	}()

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	metrics := downsample.NewDownsampleMetrics(reg)
	// Start cycle of syncing blocks from the bucket and garbage collecting the bucket.
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, insBkt, "bucket client")
			statusProber.Ready()

			return runutil.Repeat(waitInterval, ctx.Done(), func() error {
				level.Info(logger).Log("msg", "start first pass of downsampling")
				metas, _, err := metaFetcher.Fetch(ctx)
				if err != nil {
					return errors.Wrap(err, "sync before first pass of downsampling")
				}

				for _, meta := range metas {
					groupKey := meta.Thanos.GroupKey()
					metrics.Downsamples.WithLabelValues(groupKey)
					metrics.DownsampleFailures.WithLabelValues(groupKey)
				}
				if err := downsample.DownsampleBucket(ctx, logger, metrics, insBkt, metas, dataDir, downsampleConcurrency, blockFilesConcurrency, hashFunc, false); err != nil {
					return errors.Wrap(err, "downsampling failed")
				}

				level.Info(logger).Log("msg", "start second pass of downsampling")
				metas, _, err = metaFetcher.Fetch(ctx)
				if err != nil {
					return errors.Wrap(err, "sync before second pass of downsampling")
				}
				if err := downsample.DownsampleBucket(ctx, logger, metrics, insBkt, metas, dataDir, downsampleConcurrency, blockFilesConcurrency, hashFunc, false); err != nil {
					return errors.Wrap(err, "downsampling failed")
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
		httpserver.WithTLSConfig(httpTLSConfig),
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
