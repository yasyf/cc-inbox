package store

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestFingerprintChangesWithSchemaOrWireFormat(t *testing.T) {
	migrationsOnly := sha256.Sum256([]byte(strings.Join(migrations, "\x00")))
	current := fingerprint(migrations, WireFormat)
	for _, tt := range []struct {
		name  string
		other string
	}{
		{"next wire format", fingerprint(migrations, WireFormat+1)},
		{"one migration fewer", fingerprint(migrations[:len(migrations)-1], WireFormat)},
		{"migrations-only fingerprint of a binary before WireFormat", hex.EncodeToString(migrationsOnly[:])},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.other == current {
				t.Errorf("fingerprint = %s for both the current schema and %s", current, tt.name)
			}
		})
	}
}
