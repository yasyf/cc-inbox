package store

import (
	"encoding/json"
	"testing"
	"time"
)

func timestampCountWhere(f Filter, self string, now time.Time) (string, []any) {
	return f.countWhere(self, now)
}

func timestampArgsJSON(t *testing.T, args []any) string {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
