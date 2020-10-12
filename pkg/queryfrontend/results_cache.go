package queryfrontend
//
//import (
//	"context"
//	"github.com/cortexproject/cortex/pkg/chunk/cache"
//	"github.com/cortexproject/cortex/pkg/querier/queryrange"
//	"github.com/go-kit/kit/log"
//	"github.com/prometheus/client_golang/prometheus"
//	"github.com/prometheus/common/model"
//)
//
//type resultsCache struct {
//	logger   log.Logger
//	cfg      queryrange.ResultsCacheConfig
//	next     queryrange.Handler
//	cache    cache.Cache
//	limits   queryrange.Limits
//	splitter queryrange.CacheSplitter
//
//	extractor   queryrange.Extractor
//	merger      queryrange.Merger
//	shouldCache queryrange.ShouldCacheFn
//}
//
//// NewResultsCacheMiddleware creates results cache middleware from config.
//// The middleware cache result using a unique cache key for a given request (step,query,user) and interval.
//// The cache assumes that each request length (end-start) is below or equal the interval.
//// Each request starting from within the same interval will hit the same cache entry.
//// If the cache doesn't have the entire duration of the request cached, it will query the uncached parts and append them to the cache entries.
//// see `generateKey`.
//func NewResultsCacheMiddleware(
//	logger log.Logger,
//	cfg queryrange.ResultsCacheConfig,
//	splitter queryrange.CacheSplitter,
//	limits queryrange.Limits,
//	merger queryrange.Merger,
//	extractor queryrange.Extractor,
//	shouldCache queryrange.ShouldCacheFn,
//	reg prometheus.Registerer,
//) (queryrange.Middleware, error) {
//	c, err := cache.New(cfg.CacheConfig, reg, logger)
//	if err != nil {
//		return nil, err
//	}
//	if cfg.Compression == "snappy" {
//		c = cache.NewSnappy(c, logger)
//	}
//
//	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
//		return &resultsCache{
//			logger:      logger,
//			cfg:         cfg,
//			next:        next,
//			cache:       c,
//			limits:      limits,
//			merger:      merger,
//			extractor:   extractor,
//			splitter:    splitter,
//			shouldCache: shouldCache,
//		}
//	}), nil
//}
//
//func (s resultsCache) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
//	if s.shouldCache != nil && !s.shouldCache(r) {
//		return s.next.Do(ctx, r)
//	}
//
//	var (
//		key      = s.splitter.GenerateCacheKey("", r)
//		extents  []queryrange.Extent
//		response queryrange.Response
//	)
//
//	maxCacheFreshness := s.limits.MaxCacheFreshness("")
//	maxCacheTime := int64(model.Now().Add(-maxCacheFreshness))
//	if r.GetStart() > maxCacheTime {
//		return s.next.Do(ctx, r)
//	}
//
//	cached, ok := s.get(ctx, key)
//	if ok {
//		response, extents, err = s.handleHit(ctx, r, cached)
//	} else {
//		response, extents, err = s.handleMiss(ctx, r)
//	}
//
//	if err == nil && len(extents) > 0 {
//		extents, err := s.filterRecentExtents(r, maxCacheFreshness, extents)
//		if err != nil {
//			return nil, err
//		}
//		s.put(ctx, key, extents)
//	}
//
//	return response, err
//}
