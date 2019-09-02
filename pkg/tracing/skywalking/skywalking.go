package skywalking

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/tetratelabs/go2sky"
	"github.com/tetratelabs/go2sky/reporter"
	"github.com/uber/jaeger-client-go"
)
// Tracer extends opentracing.Tracer.
type Tracer struct {
	opentracing.Tracer
}

// GetTraceIDFromSpanContext return TraceID from span.Context.
func (t *Tracer) GetTraceIDFromSpanContext(ctx opentracing.SpanContext) (string, bool) {
	if c, ok := ctx.(go2sky.SegmentContext); ok {
		return fmt.Sprintf("%016x", c.TraceID().Low), true
	}
	return "", false
}

// NewTracer create tracer from YAML.
func NewTracer(ctx context.Context, logger log.Logger, conf []byte) (opentracing.Tracer, io.Closer, error) {
	r, err := reporter.NewGRPCReporter("oap-skywalking:11800")
	if err != nil {
		log.Fatalf("new reporter error %v \n", err)
	}
	defer r.Close()
	tracer, err := go2sky.NewTracer("example", go2sky.WithReporter(r))
	return tracer, r, nil
}