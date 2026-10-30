package store_test

import (
	"slices"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestUnboundPRsExcludesOnlyExplicitRecordOccurrences(t *testing.T) {
	for _, tt := range []struct {
		name  string
		topic string
		text  string
		want  []int
	}{
		{"bare", "", "#123", []int{123}},
		{"record", "", "record #123", nil},
		{"record topic", "record #123", "summary", nil},
		{"same number bare", "", "record #123 and #123", []int{123}},
		{"mixed", "", "record #123 and #456", []int{456}},
		{"word boundary", "", "prerecord #123", []int{123}},
		{"qualified", "", "record owner/repo#123", []int{123}},
		{"field boundary", "record", "#123", []int{123}},
		{"duplicates", "#123", "#123 and #456", []int{123, 456}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := store.UnboundPRs(store.Record{Topic: tt.topic, Text: tt.text})
			if !slices.Equal(got, tt.want) {
				t.Errorf("UnboundPRs = %v, want %v", got, tt.want)
			}
		})
	}
}
