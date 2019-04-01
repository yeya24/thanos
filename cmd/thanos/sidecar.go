package main

import (
	"context"
	"github.com/hashicorp/go-version"
	"github.com/prometheus/common/model"
	"math"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/promclient"
	"github.com/improbable-eng/thanos/pkg/reloader"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for Prometheus server")

	grpcBindAddr, httpBindAddr, cert, key, clientCA, newPeerFn := regCommonServerFlags(cmd)

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API. For better performance use local network.").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "Config file watched by the reloader.").
		Default("").String()

	reloaderCfgOutputFile := cmd.Flag("reloader.config-envsubst-file", "Output file for environment variable substituted config file.").
		Default("").String()

	reloaderRuleDirs := cmd.Flag("reloader.rule-dir", "Rule directories for the reloader to refresh (repeated field).").Strings()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", false)

	uploadCompacted := cmd.Flag("shipper.upload-compacted", "[Experimental] If true sidecar will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus.").Default("false").Hidden().Bool()

	validateProm := cmd.Flag("sidecar.validate-prom", "If true sidecar will check Prometheus' flags to ensure disabled compaction and 2h block-time.").Default("true").Hidden().Bool()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		rl := reloader.New(
			log.With(logger, "component", "reloader"),
			reloader.ReloadURLFromBase(*promURL),
			*reloaderCfgFile,
			*reloaderCfgOutputFile,
			*reloaderRuleDirs,
		)
		peer, err := newPeerFn(logger, reg, false, "", false)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*cert,
			*key,
			*clientCA,
			*httpBindAddr,
			*promURL,
			*dataDir,
			objStoreConfig,
			peer,
			rl,
			*uploadCompacted,
			*validateProm,
		)
	}
}

func runSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	cert string,
	key string,
	clientCA string,
	httpBindAddr string,
	promURL *url.URL,
	dataDir string,
	objStoreConfig *pathOrContent,
	peer cluster.Peer,
	reloader *reloader.Reloader,
	uploadCompacted bool,
	validateProm bool,
) error {
	var m = &promMetadata{
		promURL: promURL,

		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: 0,
		maxt: math.MaxInt64,
	}

	// Setup all the concurrent groups.
	{
		promUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		reg.MustRegister(promUp, lastHeartbeat)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			if validateProm {
				// Retry infinitely until we get Prometheus version.
				err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
					err := m.FetchPromVersion(logger)
					if err != nil {
						level.Warn(logger).Log(
							"msg", "failed to get Prometheus version. Is Prometheus running? Retrying",
							"err", err,
						)
						return errors.Wrapf(err, "fetch Prometheus version")
					}
					return nil
				})
				if err != nil {
					return err
				}

				if m.version == nil {
					level.Warn(logger).Log("msg", "can't fetch version, skip validation")
				} else {
					// Check if Prometheus has /status/flags endpoint.
					if m.version.LessThan(promclient.FlagsVersion) {
						level.Warn(logger).Log("msg",
							"Prometheus doesn't support flags endpoint, skip validation", "version", m.version.Original())
						return nil
					}

					// Check prometheus's flags to ensure sane sidecar flags.
					if err := validatePrometheus(ctx, logger, promURL, dataDir); err != nil {
						return errors.Wrap(err, "validate Prometheus flags")
					}
				}
			}

			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				if err := m.UpdateLabels(ctx, logger); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					return err
				}

				level.Info(logger).Log(
					"msg", "successfully loaded prometheus external labels",
					"external_labels", m.Labels().String(),
				)
				promUp.Set(1)
				lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(m.Labels()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured")
			}

			// New gossip cluster.
			mint, maxt := m.Timestamps()
			if err = peer.Join(cluster.PeerTypeSource, cluster.PeerMetadata{
				Labels:  m.LabelsPB(),
				MinTime: mint,
				MaxTime: maxt,
			}); err != nil {
				return errors.Wrap(err, "join cluster")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				if err := m.UpdateLabels(iterCtx, logger); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					// Update gossip.
					peer.SetLabels(m.LabelsPB())

					promUp.Set(1)
					lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				}

				return nil
			})
		}, func(error) {
			cancel()
			peer.Close(2 * time.Second)
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return reloader.Watch(ctx)
		}, func(error) {
			cancel()
		})
	}
	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}
	{
		l, err := net.Listen("tcp", grpcBindAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", component.Sidecar.String())

		var client http.Client

		promStore, err := store.NewPrometheusStore(
			logger, &client, promURL, component.Sidecar, m.Labels, m.Timestamps)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		opts, err := defaultGRPCServerOpts(logger, reg, tracer, cert, key, clientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}
		s := grpc.NewServer(opts...)
		storepb.RegisterStoreServer(s, promStore)

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
		})
	}

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	var uploads = true
	if len(confContentYaml) == 0 {
		level.Info(logger).Log("msg", "No supported bucket was configured, uploads will be disabled")
		uploads = false
	}

	if uploads {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Sidecar.String())
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		if err := promclient.IsWALDirAccesible(dataDir); err != nil {
			level.Error(logger).Log("err", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			var s *shipper.Shipper
			if uploadCompacted {
				s, err = shipper.NewWithCompacted(ctx, logger, reg, dataDir, bkt, m.Labels, metadata.SidecarSource, m.promURL)
				if err != nil {
					return errors.Wrap(err, "create shipper")
				}
			} else {
				s = shipper.New(logger, reg, dataDir, bkt, m.Labels, metadata.SidecarSource)
			}

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if uploaded, err := s.Sync(ctx); err != nil {
					level.Warn(logger).Log("err", err, "uploaded", uploaded)
				}

				minTime, _, err := s.Timestamps()
				if err != nil {
					level.Warn(logger).Log("msg", "reading timestamps failed", "err", err)
				} else {
					m.UpdateTimestamps(minTime, math.MaxInt64)

					mint, maxt := m.Timestamps()
					peer.SetTimestamps(mint, maxt)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting sidecar", "peer", peer.Name())
	return nil
}

func validatePrometheus(ctx context.Context, logger log.Logger, promURL *url.URL, tsdbPath string) error {
	flags, err := promclient.ConfiguredFlags(ctx, logger, promURL)
	if err != nil {
		return errors.Wrap(err, "failed to check flags")
	}
	// Check if min-block-time and max-block-time are 2h.
	if flags.TSDBMinTime != model.Duration(2*time.Hour) || flags.TSDBMaxTime != model.Duration(2*time.Hour) {
		return errors.Errorf("Found that TSDB Max time is %s and Min time is %s. "+
			"Compaction needs to be disabled (storage.tsdb.min-block-duration = storage.tsdb.max-block-duration = 2h)", flags.TSDBMaxTime, flags.TSDBMinTime)
	}

	return nil
}

type promMetadata struct {
	promURL *url.URL

	mtx     sync.Mutex
	mint    int64
	maxt    int64
	labels  labels.Labels
	version *version.Version
}

func (s *promMetadata) UpdateLabels(ctx context.Context, logger log.Logger) error {
	elset, err := promclient.ExternalLabels(ctx, logger, s.promURL)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.labels = elset
	return nil
}

func (s *promMetadata) UpdateTimestamps(mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.mint = mint
	s.maxt = maxt
}

func (s *promMetadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *promMetadata) LabelsPB() []storepb.Label {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := make([]storepb.Label, 0, len(s.labels))
	for _, l := range s.labels {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return lset
}

func (s *promMetadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.mint, s.maxt
}

func (s *promMetadata) FetchPromVersion(logger log.Logger) (err error) {
	s.version, err =  promclient.GetPromVersion(logger, s.promURL)
	return err
}