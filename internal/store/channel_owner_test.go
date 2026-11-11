package store_test

import (
	"fmt"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestTheNewestChannelServerOwnsItsWindowWhicheverClaimsFirst(t *testing.T) {
	window := store.Window{PID: 7807, Started: 1791277810598}
	older := store.Window{PID: 2753, Started: 1791277900000}
	newer := store.Window{PID: 96808, Started: 1791278000000}
	for _, order := range [][]store.Window{{older, newer}, {newer, older}} {
		t.Run(fmt.Sprintf("pid %d first", order[0].PID), func(t *testing.T) {
			st, _ := testutil.Store(t)
			for _, p := range order {
				if _, err := st.ClaimChannel(window, p); err != nil {
					t.Fatal(err)
				}
			}
			for _, p := range []store.Window{older, newer} {
				if owner, err := st.ClaimChannel(window, p); err != nil || owner != newer {
					t.Fatalf("ClaimChannel(%+v) = %+v, %v, want the newer server %+v", p, owner, err, newer)
				}
			}
			if owner, err := st.ClaimChannel(store.Window{PID: 5372, Started: window.Started}, older); err != nil || owner != older {
				t.Fatalf("another window's ClaimChannel = %+v, %v, want %+v", owner, err, older)
			}
		})
	}
}
