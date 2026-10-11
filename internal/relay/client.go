package relay

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
)

type StatusError struct {
	Code    int
	Kind    string
	Message string
	Body    string
}

func (e *StatusError) Error() string {
	if e.Kind != "" {
		return fmt.Sprintf("relay: %d %s: %s", e.Code, e.Kind, e.Message)
	}
	return fmt.Sprintf("relay: %d: %s", e.Code, e.Body)
}

func IsUnpaired(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Kind == "unpaired"
}

var defaultHTTP = &http.Client{Timeout: time.Minute}

type Client struct {
	Config   Config
	Identity e2e.Identity
	HTTP     *http.Client
	Now      func() time.Time
}

func (c Client) Call(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.Config.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if err := c.sign(req, path, body); err != nil {
		return err
	}
	httpc := c.HTTP
	if httpc == nil {
		httpc = defaultHTTP
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return fmt.Errorf("relay %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read relay %s %s: %w", method, path, err)
	}
	if resp.StatusCode/100 != 2 {
		se := &StatusError{Code: resp.StatusCode, Body: string(raw)}
		var ae apiError
		if json.Unmarshal(raw, &ae) == nil {
			se.Kind, se.Message = ae.Error, ae.Message
		}
		return se
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode relay %s %s: %w", method, path, err)
	}
	return nil
}

func (c Client) sign(req *http.Request, path string, body []byte) error {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("request nonce: %w", err)
	}
	ts := strconv.FormatInt(now().Unix(), 10)
	sum := sha256.Sum256(body)
	sig, err := c.Identity.Sign(e2e.RequestMessage(req.Method, path, ts, base64.StdEncoding.EncodeToString(nonce), hex.EncodeToString(sum[:])))
	if err != nil {
		return fmt.Errorf("sign relay request: %w", err)
	}
	req.Header.Set("X-CCI-Node", c.Identity.ID())
	req.Header.Set("X-CCI-Time", ts)
	req.Header.Set("X-CCI-Nonce", base64.StdEncoding.EncodeToString(nonce))
	req.Header.Set("X-CCI-Sig", base64.StdEncoding.EncodeToString(sig))
	return nil
}
