package e2e

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// SignRequest stamps req with id's relay request signature over path and body.
func SignRequest(id Identity, req *http.Request, path string, body []byte, now time.Time) error {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("request nonce: %w", err)
	}
	ts := strconv.FormatInt(now.Unix(), 10)
	sum := sha256.Sum256(body)
	sig, err := id.Sign(RequestMessage(req.Method, path, ts, b64(nonce), hex.EncodeToString(sum[:])))
	if err != nil {
		return fmt.Errorf("sign relay request: %w", err)
	}
	req.Header.Set("X-CCI-Node", id.ID())
	req.Header.Set("X-CCI-Time", ts)
	req.Header.Set("X-CCI-Nonce", b64(nonce))
	req.Header.Set("X-CCI-Sig", base64.StdEncoding.EncodeToString(sig))
	return nil
}
