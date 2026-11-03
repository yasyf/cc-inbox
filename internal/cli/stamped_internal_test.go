package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestATimeFromAnEarlierDateOrReadAsPastIsNotRefused(t *testing.T) {
	at := time.Date(2026, time.November, 3, 2, 10, 0, 0, time.Local)
	later, yesterday := at.Add(20*time.Minute).Format("3:04 PM"), at.AddDate(0, 0, -1).Format("Jan 2")
	for _, tt := range []struct {
		text    string
		refused bool
		note    bool
	}{
		{text: "a " + yesterday + " " + later + " ruling"},
		{text: "the ruling of " + later + " " + yesterday},
		{text: "it happened yesterday at " + later},
		{text: "it happened on " + at.AddDate(0, 0, -1).Format("2006-01-02") + " " + later},
		{text: "the " + later + " ruling", note: true},
		{text: "done by " + later},
		{text: "release scheduled " + later},
		{text: "it happened at " + later, refused: true},
		{text: "the deploy landed " + later, refused: true},
	} {
		var stderr bytes.Buffer
		got, err := stamped(&stderr, tt.text, "fix-lane-1", at, false)
		if tt.refused {
			if err == nil || !strings.Contains(err.Error(), "has not happened yet") {
				t.Errorf("%q = %q, %v; want it refused", tt.text, got, err)
			}
			continue
		}
		if err != nil || got != tt.text || (stderr.Len() > 0) != tt.note {
			t.Errorf("%q = %q, %v, stderr %q; want it stored as typed, noted %v", tt.text, got, err, stderr.String(), tt.note)
		}
	}
}

func TestAPastClaimAboutATimeLaterThanNowPassesWhenItCanOnlyBeEarlierToday(t *testing.T) {
	at := time.Date(2026, time.November, 3, 23, 50, 0, 0, time.Local)
	text := "it happened at " + at.Add(20*time.Minute).Format("3:04 PM")
	if _, err := stamped(&bytes.Buffer{}, text, "fix-lane-1", at, false); err != nil {
		t.Fatalf("%q at %s = %v; a time past midnight is earlier today", text, at.Format("3:04 PM"), err)
	}
}
