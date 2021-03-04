// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplarspb

import (
	"encoding/json"
	"math/big"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// UnmarshalJSON implements json.Unmarshaler.
func (m *Exemplar) UnmarshalJSON(b []byte) error {
	v := struct {
		Labels    labelpb.ZLabelSet
		TimeStamp model.Time
		Value     model.SampleValue
	}{}

	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	m.Labels = v.Labels
	m.Ts = int64(v.TimeStamp)
	m.Value = float64(v.Value)

	return nil
}

// MarshalJSON implements json.Marshaler.
func (m *Exemplar) MarshalJSON() ([]byte, error) {
	v := struct {
		Labels    labels.Labels     `json:"labels"`
		TimeStamp model.Time        `json:"timestamp"`
		Value     model.SampleValue `json:"value"`
	}{
		Labels:    labelpb.ZLabelsToPromLabels(m.Labels.Labels),
		TimeStamp: model.Time(m.Ts),
		Value:     model.SampleValue(m.Value),
	}
	return json.Marshal(v)
}

func NewExemplarsResponse(e *ExemplarData) *ExemplarsResponse {
	return &ExemplarsResponse{
		Result: &ExemplarsResponse_Data{
			Data: e,
		},
	}
}

func NewWarningExemplarsResponse(warning error) *ExemplarsResponse {
	return &ExemplarsResponse{
		Result: &ExemplarsResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

func (m *ExemplarData) Compare(other *ExemplarData) int {
	if d := labels.Compare(m.SeriesLabels.PromLabels(), other.SeriesLabels.PromLabels()); d != 0 {
		return d
	}
	return 0
}

func (m *ExemplarData) SetSeriesLabels(ls labels.Labels) {
	var result labelpb.ZLabelSet

	if len(ls) > 0 {
		result = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(ls)}
	}

	m.SeriesLabels = result
}

func (m *Exemplar) SetLabels(ls labels.Labels) {
	var result labelpb.ZLabelSet

	if len(ls) > 0 {
		result = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(ls)}
	}

	m.Labels = result
}

func (m *Exemplar) Compare(other *Exemplar) int {
	if d := labels.Compare(m.Labels.PromLabels(), other.Labels.PromLabels()); d != 0 {
		return d
	}

	if m.Hasts && other.Hasts {
		if m.Ts < other.Ts {
			return 1
		}
		if m.Ts > other.Ts {
			return -1
		}
	}

	if d := big.NewFloat(m.Value).Cmp(big.NewFloat(other.Value)); d != 0 {
		return d
	}

	return 0
}
