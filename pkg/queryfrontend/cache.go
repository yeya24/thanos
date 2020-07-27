package queryfrontend

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/cacheutil"
)

// MemcachedCache is a memcached-based cache.
type MemcachedCache struct {
	logger    log.Logger
	memcached cacheutil.MemcachedClient

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

// NewMemcachedCache makes a new MemcachedCache.
func NewMemcachedCache(name string, logger log.Logger, memcached cacheutil.MemcachedClient, reg prometheus.Registerer) *MemcachedCache {
	c := &MemcachedCache{
		logger:    logger,
		memcached: memcached,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "thanos_cache_memcached_requests_total",
		Help:        "Total number of items requests to memcached.",
		ConstLabels: prometheus.Labels{"name": name},
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name:        "thanos_cache_memcached_hits_total",
		Help:        "Total number of items requests to the cache that were a hit.",
		ConstLabels: prometheus.Labels{"name": name},
	})

	level.Info(logger).Log("msg", "created memcached cache")

	return c
}

func (c *MemcachedCache) Store(ctx context.Context, keys []string, bufs [][]byte) {
	var (
		firstErr error
		failed   int
	)

	for i, key := range keys {
		if err := c.memcached.SetAsync(ctx, key, bufs[i], 1); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		level.Warn(c.logger).Log("msg", "failed to store one or more items into memcached", "failed", failed, "firstErr", firstErr)
	}
}

func (c *MemcachedCache) Fetch(ctx context.Context, keys []string) ([]string, [][]byte, []string) {
	found := make([]string, 0, len(keys))
	missed := make([]string, 0)
	bufs := make([][]byte, 0, len(keys))

	items := c.memcached.GetMulti(ctx, keys)
	for _, key := range keys {
		item, ok := items[key]
		if ok {
			found = append(found, key)
			bufs = append(bufs, item)
		} else {
			missed = append(missed, key)
		}
	}

	return found, bufs, missed
}

func (c *MemcachedCache) Stop() {
	c.memcached.Stop()
}
