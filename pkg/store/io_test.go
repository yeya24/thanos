// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"io"
	"os"
	"testing"
)

func TestByteRanges_contiguous(t *testing.T) {
	tests := []struct {
		ranges   byteRanges
		expected bool
	}{
		{
			ranges:   nil,
			expected: true,
		}, {
			ranges:   byteRanges{{offset: 10, length: 5}},
			expected: true,
		}, {
			ranges:   byteRanges{{offset: 10, length: 5}, {offset: 15, length: 3}, {offset: 18, length: 2}},
			expected: true,
		}, {
			ranges:   byteRanges{{offset: 10, length: 3}, {offset: 15, length: 3}, {offset: 18, length: 2}},
			expected: false,
		}, {
			ranges:   byteRanges{{offset: 10, length: 5}, {offset: 15, length: 3}, {offset: 19, length: 2}},
			expected: false,
		},
	}

	for _, tc := range tests {
		testutil.Equals(t, tc.expected, tc.ranges.areContiguous())
	}
}

func TestReadByteRanges(t *testing.T) {
	tests := map[string]struct {
		src          []byte
		ranges       byteRanges
		expectedRead []byte
		expectedErr  error
	}{
		"no ranges": {
			src:          []byte(""),
			ranges:       nil,
			expectedRead: nil,
		},
		"single range with offset == 0": {
			src:          []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges:       []byteRange{{offset: 0, length: 21}},
			expectedRead: []byte("ABCDEFGHILMNOPQRSTUVZ"),
		},
		"single range with offset > 0": {
			src:          []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges:       []byteRange{{offset: 10, length: 11}},
			expectedRead: []byte("MNOPQRSTUVZ"),
		},
		"multiple contiguous ranges with first offset == 0": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 10},
				{offset: 10, length: 10},
				{offset: 20, length: 1},
			},
			expectedRead: []byte("ABCDEFGHILMNOPQRSTUVZ"),
		},
		"multiple contiguous ranges with first offset > 0": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 5, length: 5},
				{offset: 10, length: 10},
				{offset: 20, length: 1},
			},
			expectedRead: []byte("FGHILMNOPQRSTUVZ"),
		},
		"multiple non-contiguous ranges": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 3},
				{offset: 10, length: 5},
				{offset: 16, length: 1},
				{offset: 20, length: 1},
			},
			expectedRead: []byte("ABCMNOPQSZ"),
		},
		"discard bytes before the first range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 5, length: 16},
			},
			expectedRead: []byte("FGHILMNOPQRSTUVZ"),
		},
		"discard bytes after the last range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 16},
			},
			expectedRead: []byte("ABCDEFGHILMNOPQR"),
		},
		"unexpected EOF while discarding bytes": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 16},
				{offset: 25, length: 5},
			},
			expectedErr: io.ErrUnexpectedEOF,
		},
		"unexpected EOF while reading byte range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 20, length: 10},
				{offset: 40, length: 10},
			},
			expectedErr: io.ErrUnexpectedEOF,
		},
		"unexpected EOF at the beginning of a byte range": {
			src: []byte("ABCDEFGHILMNOPQRSTUVZ"),
			ranges: []byteRange{
				{offset: 0, length: 10},
				{offset: 20, length: 1},
				{offset: 21, length: 10},
			},
			expectedErr: io.ErrUnexpectedEOF,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := make([]byte, 0, 1024)
			actual, err := readByteRanges(bytes.NewReader(testData.src), actual, testData.ranges)

			if testData.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, true, errors.Is(err, testData.expectedErr))
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, testData.expectedRead, actual)
			}
		})
	}
}

func TestReadRules(t *testing.T) {
	path := "/Users/benye/rules.json"
	outputPath := "/Users/benye/queries.txt"
	outputPath2 := "/Users/benye/problem.txt"
	f, err := os.Open(path)
	testutil.Ok(t, err)
	output, err := io.ReadAll(f)
	testutil.Ok(t, err)

	var m struct {
		Data v1.RuleDiscovery `json:"data"`
	}
	err = json.Unmarshal(output, &m)
	testutil.Ok(t, err)
	var (
		count     int
		i         int
		recording int
		rgMaxSize int
	)
	outputF, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	testutil.Ok(t, err)
	outputF2, err := os.OpenFile(outputPath2, os.O_RDWR|os.O_CREATE, os.ModePerm)
	testutil.Ok(t, err)
	rgMap := make(map[string]*v1.RuleGroup)
	rgCount := make(map[string]int)
	for _, rg := range m.Data.RuleGroups {
		if _, ok := rgMap[rg.Name]; ok {
			rgMap[rg.Name].Rules = append(rgMap[rg.Name].Rules, rg.Rules...)
		} else {
			rgMap[rg.Name] = rg
		}
	}
	for _, rg := range rgMap {
		rgCount[rg.Name] = 0
		strs := []string{}
		for _, rule := range rg.Rules {
			mmmm, ok := rule.(map[string]interface{})
			if ok {
				if mmmm["type"] != "alerting" {
					recording++
				}
				s := fmt.Sprintf("rg: %s type: %s query: %s\n", rg.Name, mmmm["type"], mmmm["query"])
				_, err = outputF.WriteString(s)
				testutil.Ok(t, err)
				i++
				expr, err := parser.ParseExpr(mmmm["query"].(string))
				testutil.Ok(t, err)

				//var hitSG bool
				//parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
				//	matrix, ok := node.(*parser.MatrixSelector)
				//	if ok {
				//		if matrix.Range > time.Hour*24 {
				//			hitSG = true
				//		}
				//	}
				//	sub, ok := node.(*parser.SubqueryExpr)
				//	if ok {
				//		if sub.Range > time.Hour*24 {
				//			hitSG = true
				//		}
				//	}
				//	return nil
				//})
				//if !hitSG {
				//	continue
				//}
				var (
					unequalMatchers bool
					regexPlus       bool
					matchAll        bool
				)
				issues := []string{}
				parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
					vector, ok := node.(*parser.VectorSelector)
					if !ok {
						return nil
					}
					if len(vector.LabelMatchers) == 1 && vector.LabelMatchers[0].Type == labels.MatchRegexp && vector.LabelMatchers[0].Value == ".*" {
						matchAll = true
					}
					for _, matcher := range vector.LabelMatchers {
						matcher := matcher
						if matcher.Type == labels.MatchNotEqual && matcher.Value == "" {
							unequalMatchers = true
						}
						if matcher.Type == labels.MatchRegexp && matcher.Value == ".+" {
							regexPlus = true
						}
					}
					return nil
				})
				if unequalMatchers {
					issues = append(issues, "unequal empty")
				}
				if matchAll {
					issues = append(issues, "matching All")
				}
				if regexPlus {
					issues = append(issues, "regex .+")
				}
				if unequalMatchers || regexPlus || matchAll {
					if _, ok := rgCount[rg.Name]; ok {
						rgCount[rg.Name]++
					} else {
						rgCount[rg.Name] = 1
					}
					s := fmt.Sprintf("query: %s issues: [%v]\n", expr, issues)
					strs = append(strs, s)
					//_, err = outputF2.WriteString(s)
					//testutil.Ok(t, err)
				}
			}
			count++
		}
		if rgCount[rg.Name] > 0 {
			s := fmt.Sprintf("RuleGroup %s rules %d Bad Rules %d\n", rg.Name, len(rg.Rules), rgCount[rg.Name])
			_, err = outputF2.WriteString(s)
			testutil.Ok(t, err)
			for _, str := range strs {
				_, err = outputF2.WriteString(str)
				testutil.Ok(t, err)
			}
		}
		if len(rg.Rules) > rgMaxSize {
			rgMaxSize = len(rg.Rules)
		}
	}
	s := fmt.Sprintf("total %d printed %d recording %d max size rule group %d\n", count, i, recording, rgMaxSize)
	_, err = outputF.WriteString(s)
	testutil.Ok(t, err)
	outputF.Close()
	outputF2.Close()
}

func TestReadRulesFilter(t *testing.T) {
	path := "/Users/benye/rules.json"
	f, err := os.Open(path)
	testutil.Ok(t, err)
	output, err := io.ReadAll(f)
	testutil.Ok(t, err)

	var m struct {
		Data v1.RuleDiscovery `json:"data"`
	}
	err = json.Unmarshal(output, &m)
	testutil.Ok(t, err)
	var (
		count     int
		recording int
		rgMaxSize int
	)
	for _, rg := range m.Data.RuleGroups {
		if rg.Name != "stream-infrastructure-alert-group" {
			continue
		}
		for _, rule := range rg.Rules {
			mmmm, ok := rule.(map[string]interface{})
			if ok {
				if mmmm["type"] != "alerting" {
					recording++
				}
				fmt.Printf("type: %s query: %s\n", mmmm["type"], mmmm["query"])
			}
			count++
		}
		if len(rg.Rules) > rgMaxSize {
			rgMaxSize = len(rg.Rules)
		}
	}
	fmt.Printf("total %d\n", count)
}
