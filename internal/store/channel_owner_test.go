package store_test

import (
	"fmt"
	"slices"
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
				if _, err := st.ClaimChannel(window, p, true); err != nil {
					t.Fatal(err)
				}
			}
			for _, p := range []store.Window{older, newer} {
				if owner, err := st.ClaimChannel(window, p, true); err != nil || owner != newer {
					t.Fatalf("ClaimChannel(%+v) = %+v, %v, want the newer server %+v", p, owner, err, newer)
				}
			}
			if owner, err := st.ClaimChannel(store.Window{PID: 5372, Started: window.Started}, older, true); err != nil || owner != older {
				t.Fatalf("another window's ClaimChannel = %+v, %v, want %+v", owner, err, older)
			}
		})
	}
}

func TestAToollessChannelServerOutranksANewerOneServingTools(t *testing.T) {
	window := store.Window{PID: 15278, Started: 1791277810598}
	toolless := store.Window{PID: 38123, Started: 1791277900000}
	withTools := store.Window{PID: 46486, Started: 1791278000000}
	newerToolless := store.Window{PID: 51200, Started: 1791278100000}
	for _, toollessFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("toolless first %v", toollessFirst), func(t *testing.T) {
			st, _ := testutil.Store(t)
			claims := []func() (store.Window, error){
				func() (store.Window, error) { return st.ClaimChannel(window, toolless, true) },
				func() (store.Window, error) { return st.ClaimChannel(window, withTools, false) },
			}
			if !toollessFirst {
				slices.Reverse(claims)
			}
			for _, claim := range claims {
				if _, err := claim(); err != nil {
					t.Fatal(err)
				}
			}
			if owner, err := st.ClaimChannel(window, withTools, false); err != nil || owner != toolless {
				t.Fatalf("ClaimChannel by the server with tools = %+v, %v, want the toolless server %+v", owner, err, toolless)
			}
			if owner, err := st.ClaimChannel(window, newerToolless, true); err != nil || owner != newerToolless {
				t.Fatalf("ClaimChannel by a newer toolless server = %+v, %v, want it to own the window", owner, err)
			}
		})
	}
}
