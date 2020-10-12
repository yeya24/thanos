// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

//// ThanosSeriesResponse represents a series response.
//type ThanosSeriesResponse struct {
//	Status    string          `json:"status"`
//	Data      []labels.Labels `json:"data,omitempty"`
//	ErrorType string          `json:"errorType,omitempty"`
//	Error     string          `json:"error,omitempty"`
//}
//
//func (r ThanosSeriesResponse) Reset()         {}
//func (r ThanosSeriesResponse) String() string { return "" }
//func (r ThanosSeriesResponse) ProtoMessage()  {}

// PrometheusResponseExtractor helps extracting specific info from Query Response.
type ThanosResponseExtractor struct{}

// Extract extracts response for specific a range from a response.
func (ThanosResponseExtractor) Extract(start, end int64, resp queryrange.Response) queryrange.Response {
	return resp
}

// ResponseWithoutHeaders just returns the original response since Thanos responses don't have HTTP headers.
func (ThanosResponseExtractor) ResponseWithoutHeaders(resp queryrange.Response) queryrange.Response {
	return resp
}
