package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestEmbedTailToWritesWhatTheEncoderWrites(t *testing.T) {
	st, _ := testutil.Store(t)
	for i := range embedPageRows + 3 {
		testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Ask, To: []string{"worker"}, Text: fmt.Sprintf("<%d> & more", i)})
	}
	for _, tt := range []struct {
		name string
		call EmbedCall
	}{
		{"no rows", EmbedCall{Op: "tail", Drive: "missing"}},
		{"one page", EmbedCall{Op: "tail", Drive: "d", After: 10, Lanes: []string{"root"}, Kinds: []string{"ask"}, PR: ""}},
		{"pages", EmbedCall{Op: "tail", Drive: "d"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			answer, err := embedAnswer(t.Context(), st, tt.call)
			if err != nil {
				t.Fatal(err)
			}
			var want, got bytes.Buffer
			if err := json.NewEncoder(&want).Encode(answer); err != nil {
				t.Fatal(err)
			}
			if err := embedTailTo(t.Context(), st, tt.call, &got); err != nil {
				t.Fatal(err)
			}
			if got.String() != want.String() {
				t.Errorf("streamed %d bytes, encoder wrote %d", got.Len(), want.Len())
			}
		})
	}
}
