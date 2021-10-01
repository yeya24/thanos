package pushdown

import (
	"context"
	"encoding/json"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/pushdown/querypb"
	"net/url"
	"time"
)

type PromQuerier struct {
	client *promclient.Client
	u      *url.URL
}

func NewPromQuerier(client *promclient.Client) *PromQuerier {
	return &PromQuerier{client: client}
}

type queryData2 struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     model.Matrix     `json:"result"`
}

func (s *PromQuerier) Query(ctx context.Context, r *querypb.QueryRequest) (*querypb.QueryResponse, error) {
	expr, err := parser.ParseExpr(r.Query)
	if err != nil {
		return nil, err
	}
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		switch n := node.(type) {
		case *parser.MatrixSelector:
			// inject labelselector
			if vs, ok := n.VectorSelector.(*parser.VectorSelector); ok {
				lbls := make([]*labels.Matcher, 0, len(vs.LabelMatchers))
				for _, ms := range vs.LabelMatchers {
					if ms.Name == "cluster" {
						continue
					} else {
						lbls = append(lbls, ms)
					}
				}
				vs.LabelMatchers = lbls
			}

		case *parser.VectorSelector:
			// inject labelselector
			lbls := make([]*labels.Matcher, 0, len(n.LabelMatchers))
			for _, ms := range n.LabelMatchers {
				if ms.Name == "cluster" {
					continue
				} else {
					lbls = append(lbls, ms)
				}
			}
			n.LabelMatchers = lbls
		}
		return nil
	})

	res, _, err := s.client.QueryRange(ctx, s.u, expr.String(), r.StartNs/int64(time.Millisecond), r.EndNs/int64(time.Millisecond), r.Interval/int64(time.Second), promclient.QueryOptions{})
	if err != nil {
		return nil, err
	}

	qData := queryData2{
		Result:     res,
		ResultType: parser.ValueTypeMatrix,
	}

	marshalledResp, err := json.Marshal(qData)
	if err != nil {
		return nil, err
	}
	return &querypb.QueryResponse{Response: string(marshalledResp)}, nil
}
