package store

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestBoardOriginalRekeyNecessarySuperset(t *testing.T) {
	if os.Getenv("CCI_BOARD_REKEY_CONTRACT") != "1" {
		t.Skip("set CCI_BOARD_REKEY_CONTRACT=1 for original rekey controls")
	}
	cases := []struct{ name, pr, row, text string }{
		{"shorter-row-token", "123", "merge-row-123-abcdef0", "re-key merge-row-123 at abcdef0123456789"},
		{"raw-leading-zero-row-pr", "00123", "merge-row-00123-abcdef0", "re-key merge-row-00123 at abcdef0123456789"},
		{"unicode-row-token", "１２３", "merge-row-１２３", "re-key merge-row-１２３"},
		{"longer-row-token-substring", "123", "merge-row-123", "re-key merge-row-1234"},
	}
	rawCases := make([]map[string]string, 0, len(cases))
	for _, one := range cases {
		rawCases = append(rawCases, map[string]string{"pr": one.pr, "row": one.row, "text": one.text})
	}
	input, err := json.Marshal(rawCases)
	if err != nil {
		t.Fatal(err)
	}
	reference := exec.CommandContext(t.Context(), "python3", "-I", "-B", filepath.Join("testdata", "board_lr845_rekey_reference.py"))
	reference.Stdin = bytes.NewReader(input)
	output, err := reference.CombinedOutput()
	if err != nil {
		t.Fatalf("actual845 rekey reference failed: %v output=%s", err, output)
	}
	var admitted []bool
	if err := json.Unmarshal(output, &admitted); err != nil {
		t.Fatal(err)
	}
	if len(admitted) != len(cases) {
		t.Fatalf("actual845 reference cases=%d expected=%d", len(admitted), len(cases))
	}
	for i, ok := range admitted {
		if !ok {
			t.Fatalf("actual845 rejected reference case=%s", cases[i].name)
		}
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			st, index, spec, _ := boardFixture(t)
			ruling, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Decision, Text: one.text}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := index.Provision(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
			err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{one.pr}, RowIDs: []string{one.row}}, func(_ *BoardSnapshot, result *BoardResult) error {
				if result.Rulings.Error != nil {
					t.Fatal(result.Rulings.Error)
				}
				if len(result.Rulings.Records) != 1 || result.Rulings.Records[0].Seq != ruling.Seq {
					t.Fatalf("BOARD_ORIGINAL_REKEY_SUPERSET_MISSING case=%s selected=%v wanted=%d", one.name, result.Rulings.Records, ruling.Seq)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("BOARD_ORIGINAL_REKEY_PARITY case=%s pr=%s row=%s", one.name, one.pr, one.row)
		})
	}
}
