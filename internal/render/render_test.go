package render_test

import (
	"testing"

	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestWithBodyIndentsTheBodyUnderTheLine(t *testing.T) {
	r := store.Record{Seq: 3, Body: []byte(`{"phase":"live","links":["a"]}`)}
	want := "#3 line\n{\n  \"phase\": \"live\",\n  \"links\": [\n    \"a\"\n  ]\n}"
	if got := render.WithBody("#3 line", r); got != want {
		t.Fatalf("WithBody() = %q, want %q", got, want)
	}
	if got := render.WithBody("#4 line", store.Record{Seq: 4}); got != "#4 line" {
		t.Fatalf("WithBody(no body) = %q", got)
	}
}
