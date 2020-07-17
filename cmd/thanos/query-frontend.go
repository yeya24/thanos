// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"time"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"gopkg.in/alecthomas/kingpin.v2"

	v1 "github.com/thanos-io/thanos/pkg/api/queryfrontend"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
)

type queryFrontendConfig struct {
	http                      httpConfig
	defaultEvaluationInterval time.Duration
	downstreamURL             string
	compressResponses         bool
	queryRangeConfig          queryRangeConfig
	indexCacheSize            units.Base2Bytes
	indexCacheConfig          *extflag.PathOrContent
}

type queryRangeConfig struct {
	respCacheConfig     responseCacheConfig
	splitInterval       time.Duration
	disableStepAlign    bool
	maxRetries          int
	maxQueryParallelism int
	maxQueryLength      time.Duration
}

type responseCacheConfig struct {
	cacheTTL          time.Duration
	cacheMaxFreshness time.Duration
}

func (c *responseCacheConfig) registerFlag(cmd *kingpin.CmdClause) {
	cmd.Flag("query-range.response-cache-ttl", "").DurationVar(&c.cacheTTL)
	cmd.Flag("query-range.response-cache-max-freshness", "").DurationVar(&c.cacheMaxFreshness)
}

func (c *queryRangeConfig) registerFlag(cmd *kingpin.CmdClause) {
	c.respCacheConfig.registerFlag(cmd)
	cmd.Flag("query-range.split-interval", "").Default("24h").DurationVar(&c.splitInterval)
	cmd.Flag("query-range.max-retries-per-request", "").Default("5").IntVar(&c.maxRetries)
	cmd.Flag("query-range.disable-step-align", "").Default("false").BoolVar(&c.disableStepAlign)
}

func (c *queryFrontendConfig) registerFlag(cmd *kingpin.CmdClause) {
	c.queryRangeConfig.registerFlag(cmd)
	c.http.registerFlag(cmd)

	cmd.Flag("query.default-evaluation-interval", "Set default evaluation interval for sub queries.").Default("1m").DurationVar(&c.defaultEvaluationInterval)

	cmd.Flag("index-cache-size", "Maximum size of items held in the in-memory index cache. Ignored if --index-cache.config or --index-cache.config-file option is specified.").
		Default("250MB").BytesVar(&c.indexCacheSize)

	cmd.Flag("query-frontend.downstream-url", "").Default("http://localhost:9090").StringVar(&c.downstreamURL)

	cmd.Flag("query-frontend.compress-responses", "").Default("false").BoolVar(&c.compressResponses)

	c.indexCacheConfig = extflag.RegisterPathOrContent(cmd, "index-cache.config",
		"YAML file that contains index cache configuration. See format details: https://thanos.io/components/store.md/#index-cache",
		false)
}

func registerQueryFrontend(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.QueryFrontend
	cmd := app.Command(comp.String(), "query frontend")
	conf := &queryFrontendConfig{}
	conf.registerFlag(cmd)

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {

		return runQueryFrontend(
			g,
			logger,
			reg,
			conf,
			comp,
		)
	}
}

func runQueryFrontend(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	conf *queryFrontendConfig,
	comp component.Component,
) error {

	fe, err := frontend.New(frontend.Config{
		DownstreamURL:     conf.downstreamURL,
		CompressResponses: conf.compressResponses,
	}, logger, reg)
	if err != nil {
		return err
	}

	limiter := queryfrontend.NewLimiter(
		conf.queryRangeConfig.maxQueryParallelism,
		conf.queryRangeConfig.maxQueryLength,
		conf.queryRangeConfig.respCacheConfig.cacheMaxFreshness,
	)

	tripperWare, err := queryfrontend.NewTripperWare(
		limiter,
		queryrange.PrometheusCodec,
		queryrange.PrometheusResponseExtractor{},
		conf.queryRangeConfig.disableStepAlign,
		conf.queryRangeConfig.splitInterval,
		conf.queryRangeConfig.maxRetries,
		reg,
		logger,
	)
	if err != nil {
		return err
	}

	fe.Wrap(tripperWare)

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start metrics HTTP server.
	{
		router := route.New()

		api := v1.NewAPI(logger)
		api.Register(router.WithPrefix("/api/v1"), fe.Handler().ServeHTTP)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(conf.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
		)
		srv.Handle("/", router)

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
	}

	return nil
}
