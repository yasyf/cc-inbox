package kinds_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const head = "e718a7434fa01ee9835f992a9a647a825350732c"

func body(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestTypedKindsAreTheTwelveInTheTable(t *testing.T) {
	for _, name := range []string{"head", "grade", "tap", "verdict", "item", "done", "escalation", "decision", "ask", "answer", "note", "state"} {
		if _, ok := kinds.Typed(kinds.Kind("lr." + name)); !ok {
			t.Errorf("lr.%s is not a typed kind", name)
		}
	}
	if _, ok := kinds.Typed(kinds.Kind("lr.evidence")); ok {
		t.Error("lr.evidence is not in the table but was accepted")
	}
	if _, ok := kinds.Typed(kinds.Ask); ok {
		t.Error("a fixed-vocabulary kind is not typed")
	}
}

func TestAWellFormedGradeIsAccepted(t *testing.T) {
	if err := kinds.CheckBody(kinds.Kind("lr.grade"), body(t, map[string]any{"pr": 45821, "head": head, "grader": "finder", "verdict": "clear"}), []string{"supervisor"}); err != nil {
		t.Fatalf("CheckBody = %v", err)
	}
}

func TestAClearPostedAsTheWrongKindNamesTheRightOne(t *testing.T) {
	err := kinds.CheckBody(kinds.Kind("lr.note"), body(t, map[string]any{"pr": 45821, "head": head, "grader": "finder", "verdict": "clear"}), nil)
	if err == nil || !strings.HasPrefix(err.Error(), "these fields are a grade record, not note; run: lr post grade") {
		t.Fatalf("CheckBody = %v", err)
	}
}

func TestAGradeNamingSeveralPRsIsRefused(t *testing.T) {
	err := kinds.CheckBody(kinds.Kind("lr.grade"), body(t, map[string]any{"pr": []int{1, 2, 3}, "head": head, "grader": "finder", "verdict": "clear"}), nil)
	if err == nil || !strings.HasPrefix(err.Error(), "a grade names one PR; post one record per PR") {
		t.Fatalf("CheckBody = %v", err)
	}
}

func TestRootHearsOnlyEscalationsAsksAndDecisions(t *testing.T) {
	refused := map[string]map[string]any{
		"state": {"lane": "lane-7"},
		"done":  {"item": 3},
		"grade": {"pr": 1, "head": head, "grader": "finder", "verdict": "clear"},
		"item":  {"item": 3, "action": "restack", "pr": 1, "head": head, "mover": "lane", "deadline": "2026-10-31T06:20:00Z"},
		"note":  {"text": "landing is slow"},
	}
	for name, fields := range refused {
		err := kinds.CheckBody(kinds.Kind("lr."+name), body(t, fields), []string{"root"})
		if err == nil || !strings.Contains(err.Error(), "never reaches root") {
			t.Errorf("lr.%s to root: CheckBody = %v", name, err)
		}
	}
	allowed := map[string]map[string]any{
		"escalation": {"subject": "org/repo#45470", "cause": "verdict overdue"},
		"ask":        {"text": "which base?"},
		"decision":   {"text": "land the bottom"},
	}
	for name, fields := range allowed {
		if err := kinds.CheckBody(kinds.Kind("lr."+name), body(t, fields), []string{"root"}); err != nil {
			t.Errorf("lr.%s to root: CheckBody = %v", name, err)
		}
	}
}

func TestFieldShapesAreChecked(t *testing.T) {
	cases := map[string]map[string]any{
		"grade.head must be a full 40-character sha":             {"pr": 1, "head": "e718a74", "grader": "finder", "verdict": "clear"},
		"grade.pr must be an integer":                            {"pr": "45821", "head": head, "grader": "finder", "verdict": "clear"},
		"grade.verdict must be one of clear, ask, pass, fail":    {"pr": 1, "head": head, "grader": "finder", "verdict": "lgtm"},
		"grade takes pr, head, grader, verdict; missing verdict": {"pr": 1, "head": head, "grader": "finder"},
	}
	for want, fields := range cases {
		err := kinds.CheckBody(kinds.Kind("lr.grade"), body(t, fields), nil)
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("CheckBody = %v; want prefix %q", err, want)
		}
	}
}

func TestATapByRootIsNotATap(t *testing.T) {
	err := kinds.CheckBody(kinds.Kind("lr.tap"), body(t, map[string]any{"pr": 45911, "head": head, "by": "root", "value": "tapped"}), []string{"supervisor"})
	if err == nil || !strings.HasPrefix(err.Error(), "tap.by must be one of owner, rule") {
		t.Fatalf("CheckBody = %v", err)
	}
}

func TestAnOptionalFieldIsAllowed(t *testing.T) {
	if err := kinds.CheckBody(kinds.Kind("lr.tap"), body(t, map[string]any{"pr": 44989, "head": head, "by": "rule", "value": "tapped", "rule": "test-only"}), []string{"supervisor"}); err != nil {
		t.Fatal(err)
	}
}

func TestAnUntypedServiceKindIsNotChecked(t *testing.T) {
	if err := kinds.CheckBody(kinds.Kind("oncall.fix.status"), json.RawMessage(`{"anything": true}`), []string{"root"}); err != nil {
		t.Fatal(err)
	}
}

func TestATypedKindNeedsABody(t *testing.T) {
	err := kinds.CheckBody(kinds.Kind("lr.grade"), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "needs --body-file") {
		t.Fatalf("CheckBody = %v", err)
	}
}
