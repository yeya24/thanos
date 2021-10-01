// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pushdown

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/pushdown/querypb"
	"github.com/thanos-io/thanos/pkg/store"
)

// TimeBasedPushdown is a type of pushdown that pushes
// down a query when a timerange of a query only
// matches one QueryAPI.
type TimeBasedPushdown struct {
	stores         func() []store.Client
	matchedQueries prometheus.Counter
}

func NewTimeBasedPushdown(
	stores func() []store.Client,
	reg prometheus.Registerer,
) *TimeBasedPushdown {
	ret := &TimeBasedPushdown{
		stores: stores,
	}
	if reg != nil {
		ret.matchedQueries = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "matches_total",
				Help: "How many queries were matched and were pushed down as a result",
			},
		)
	}
	return ret
}

type pushdownStore struct {
	c       store.Client
	extLbls []labels.Labels
}

// Match finds a querypb.QueryClient that we can push down the query
// to if it is the only partially matching QueryAPI & StoreAPI tuple.
// It returns false if nothing has been found.
func (pa *TimeBasedPushdown) Match(query string, startNS, endNS int64) (querypb.QueryClient, bool) {
	stores := pa.stores()

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, false
	}
	sts := make([]*pushdownStore, 0, len(stores))
	for _, st := range stores {
		mint, maxt := st.TimeRange()

		if !(mint <= endNS/int64(time.Millisecond) && startNS/int64(time.Millisecond) <= maxt) {
			continue
		}

		if st.QueryAPI() != nil {
			sts = append(sts, &pushdownStore{c: st, extLbls: st.LabelSets()})
		}
	}

	checker := NewChecker(sts)
	checker.EnforceNode(expr)
	if len(checker.stores) == 1 {
		pa.matchedQueries.Inc()
		return checker.stores[0].c.QueryAPI(), true
	}

	return nil, false
}

type Checker struct {
	stores []*pushdownStore
}

func NewChecker(stores []*pushdownStore) *Checker {
	return &Checker{
		stores: stores,
	}
}

func (ms *Checker) EnforceNode(node parser.Node) {
	switch n := node.(type) {
	case *parser.EvalStmt:
		ms.EnforceNode(n.Expr)

	case parser.Expressions:
		for _, e := range n {
			ms.EnforceNode(e)
		}

	case *parser.AggregateExpr:
		ms.EnforceNode(n.Expr)

	case *parser.BinaryExpr:
		ms.EnforceNode(n.LHS)

		ms.EnforceNode(n.RHS)

	case *parser.Call:
		ms.EnforceNode(n.Args)

	case *parser.SubqueryExpr:
		ms.EnforceNode(n.Expr)

	case *parser.ParenExpr:
		ms.EnforceNode(n.Expr)

	case *parser.UnaryExpr:
		ms.EnforceNode(n.Expr)

	case *parser.NumberLiteral, *parser.StringLiteral:
	// nothing to do

	case *parser.MatrixSelector:
		// inject labelselector
		if vs, ok := n.VectorSelector.(*parser.VectorSelector); ok {
			ms.EnforceMatchers(vs.LabelMatchers)
		}

	case *parser.VectorSelector:
		// inject labelselector
		ms.EnforceMatchers(n.LabelMatchers)

	default:
		panic(fmt.Errorf("parser.Walk: unhandled node type %T", n))
	}

}

// EnforceMatchers appends the configured label matcher if not present.
// If the label matcher that is to be injected is present (by labelname) but
// different (either by match type or value) the behavior depends on the
// errorOnReplace variable. If errorOnReplace is true an error is returned,
// otherwise the label matcher is silently replaced.
func (ms *Checker) EnforceMatchers(targets []*labels.Matcher) {
	//stores := make([]*pushdownStore, 0, len(ms.stores))
	temp := ms.stores[:0]
	for _, s := range ms.stores {
		if labelSetsMatch(targets, s.extLbls...) {
			temp = append(temp, s)
		}
	}
	//// Prevent memory leak by erasing truncated values
	//// (not needed if values don't contain pointers, directly or indirectly)
	//for j := i; j < len(ms.stores); j++ {
	//	ms.stores[j] = nil
	//}
	ms.stores = temp
}

// labelSetsMatch returns false if all label-set do not match the matchers (aka: OR is between all label-sets).
func labelSetsMatch(matchers []*labels.Matcher, lset ...labels.Labels) bool {
	if len(lset) == 0 {
		return true
	}

	for _, ls := range lset {
		notMatched := false
		for _, m := range matchers {
			if lv := ls.Get(m.Name); lv != "" && !m.Matches(lv) {
				notMatched = true
				break
			}
		}
		if !notMatched {
			return true
		}
	}
	return false
}

func matchesExternalLabels(tms []*labels.Matcher, externalLabels labels.Labels) (bool, []*labels.Matcher) {
	if len(externalLabels) == 0 {
		return true, tms
	}

	var newMatchers []*labels.Matcher
	for i, tm := range tms {
		// Validate all matchers.
		extValue := externalLabels.Get(tm.Name)
		if extValue == "" {
			// Agnostic to external labels.
			tms = append(tms[:i], tms[i:]...)
			newMatchers = append(newMatchers, tm)
			continue
		}

		if !tm.Matches(extValue) {
			// External label does not match. This should not happen - it should be filtered out on query node,
			// but let's do that anyway here.
			return false, nil
		}
	}
	return true, newMatchers
}
