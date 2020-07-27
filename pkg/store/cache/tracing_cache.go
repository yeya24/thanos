// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/thanos-io/thanos/pkg/tracing"
)

// tracingCache includes Fetch operation in the traces.
type tracingCache struct {
	c Cache
}

func newTracingCache(cache Cache) Cache {
	return tracingCache{c: cache}
}

func (t tracingCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	t.c.Store(ctx, data, ttl)
}

func (t tracingCache) Fetch(ctx context.Context, keys []string) (result map[string][]byte) {
	tracing.DoWithSpan(ctx, "cache_fetch", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("requested keys", len(keys))

		result = t.c.Fetch(spanCtx, keys)

		bytes := 0
		for _, v := range result {
			bytes += len(v)
		}
		span.LogKV("returned keys", len(result), "returned bytes", bytes)
	})
	return
}
