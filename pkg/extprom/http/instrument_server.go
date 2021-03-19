// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package http

import (
	"github.com/felixge/httpsnoop"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaveworks/common/middleware"
	"net/http"
	"strconv"
)

// InstrumentationMiddleware holds necessary metrics to instrument an http.Server
// and provides necessary behaviors.
type InstrumentationMiddleware interface {
	// NewHandler wraps the given HTTP handler for instrumentation.
	NewHandler(handlerName string, handler http.Handler) http.HandlerFunc
}

type nopInstrumentationMiddleware struct{}

func (ins nopInstrumentationMiddleware) NewHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
	})
}

// NewNopInstrumentationMiddleware provides a InstrumentationMiddleware which does nothing.
func NewNopInstrumentationMiddleware() InstrumentationMiddleware {
	return nopInstrumentationMiddleware{}
}

type defaultInstrumentationMiddleware struct {
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.SummaryVec
	requestsTotal   *prometheus.CounterVec
	responseSize    *prometheus.SummaryVec
}

// NewInstrumentationMiddleware provides default InstrumentationMiddleware.
func NewInstrumentationMiddleware(reg prometheus.Registerer) InstrumentationMiddleware {
	ins := defaultInstrumentationMiddleware{
		requestDuration: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "Tracks the latencies for HTTP requests.",
				Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
			},
			[]string{"code", "handler", "method"},
		),

		requestSize: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "http_request_size_bytes",
				Help: "Tracks the size of HTTP requests.",
			},
			[]string{"code", "handler", "method"},
		),

		requestsTotal: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Tracks the number of HTTP requests.",
			}, []string{"code", "handler", "method"},
		),

		responseSize: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "http_response_size_bytes",
				Help: "Tracks the size of HTTP responses.",
			},
			[]string{"code", "handler", "method"},
		),
	}
	return &ins
}

// NewHandler wraps the given HTTP handler for instrumentation. It
// registers four metric collectors (if not already done) and reports HTTP
// metrics to the (newly or already) registered collectors: http_requests_total
// (CounterVec), http_request_duration_seconds (Histogram),
// http_request_size_bytes (Summary), http_response_size_bytes (Summary). Each
// has a constant label named "handler" with the provided handlerName as
// value. http_requests_total is a metric vector partitioned by HTTP method
// (label name "method") and HTTP status code (label name "code").
func (ins *defaultInstrumentationMiddleware) NewHandler(handlerName string, handler http.Handler) http.HandlerFunc {
	return ins.instrumentHandlerDuration(
		ins.requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerRequestSize(
			ins.requestSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			promhttp.InstrumentHandlerCounter(
				ins.requestsTotal.MustCurryWith(prometheus.Labels{"handler": handlerName}),
				promhttp.InstrumentHandlerResponseSize(
					ins.responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
					handler,
				),
			),
		),
	)
}

func (ins *defaultInstrumentationMiddleware) instrumentHandlerDuration(obs prometheus.ObserverVec, next http.Handler) http.HandlerFunc {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respMetrics := httpsnoop.CaptureMetricsFn(w, func(ww http.ResponseWriter) {
			next.ServeHTTP(ww, r)
		})

		histogram := obs.With(prometheus.Labels{"code": strconv.Itoa(respMetrics.Code), "method": r.Method})
		if traceID, ok := middleware.ExtractSampledTraceID(r.Context()); ok {
			// Need to type-convert the Observer to an
			// ExemplarObserver. This will always work for a
			// HistogramVec.
			histogram.(prometheus.ExemplarObserver).ObserveWithExemplar(
				respMetrics.Duration.Seconds(), prometheus.Labels{"traceID": traceID},
			)
			return
		}
		histogram.Observe(respMetrics.Duration.Seconds())
	})
}
