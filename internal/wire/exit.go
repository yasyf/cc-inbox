package wire

import "encoding/binary"

// ExitPayload encodes an exit code for a FrameExit.
func ExitPayload(code int) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(code)) //nolint:gosec // G115: exit codes are small.
}
