package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
)

const followSilence = 90 * time.Second

// Follows reads the wakes of the daemon listening on addr for the store at
// home. A daemon that is absent, too old to know them, serving another store,
// or silent past three heartbeats answers store.ErrUnrouted, so the reader
// looks for commits itself.
func Follows(home, addr string) store.Feed {
	return &follows{home: home, url: "http://" + addr + server.FollowPath, http: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}}
}

type follows struct {
	home string
	url  string
	http *http.Client
}

func (r *follows) Follow(ctx context.Context, f store.Filter, each func(store.Wake) error) error {
	body, err := json.Marshal(server.FollowRequest{Home: r.home, Filter: f})
	if err != nil {
		return fmt.Errorf("encode follow: %w", err)
	}
	read, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(read, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build follow: %w", err)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", store.ErrUnrouted, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: follow answered %s", store.ErrUnrouted, resp.Status)
	}
	silent := time.AfterFunc(followSilence, cancel)
	defer silent.Stop()
	lines := json.NewDecoder(resp.Body)
	for {
		var wake store.Wake
		if err := lines.Decode(&wake); err != nil {
			return fmt.Errorf("%w: %w", store.ErrUnrouted, err)
		}
		silent.Stop()
		if wake.Head != 0 {
			if err := each(wake); err != nil {
				return err
			}
		}
		silent.Reset(followSilence)
	}
}
