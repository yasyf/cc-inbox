package store

import (
	"context"
	"testing"
)

func TestPeerRoleDerivedForRowsBoundBeforeRoles(t *testing.T) {
	st, err := Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	for _, stmt := range []string{
		"INSERT INTO sessions (session, drive, root) VALUES ('old-root', 'd', 1)",
		"INSERT INTO sessions (session, drive, root, lane) VALUES ('old-lane', 'd', 0, 'api')",
		"INSERT INTO sessions (session, drive, root) VALUES ('old-plain', 'd', 0)",
	} {
		if _, err := st.db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	for session, want := range map[string]string{"old-root": "root", "old-lane": "lane", "old-plain": "session"} {
		p, err := st.Peer(ctx, session)
		if err != nil || p.Role != want {
			t.Fatalf("Peer(%s).Role = %q, %v; want %q", session, p.Role, err, want)
		}
	}
}
