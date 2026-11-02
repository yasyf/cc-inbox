package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/yasyf/cc-inbox/internal/store"
)

func (request nativeBoardRequest) originalReadError(ctx context.Context, st *store.Store, category int) error {
	argv := boardStrings([][]*string{request.RulingArgv, request.ReviewArgv, request.WithdrawArgv}[category])
	peer := store.PeerKey{}
	if *request.Session != "" {
		peer = store.PeerKey{Session: *request.Session, Agent: *request.Agent}
	}
	cmd, flags := tailCommand(peer.String())
	if err := cmd.ParseFlags(argv[2:]); err != nil {
		return err
	}
	for {
		var stdout, stderr bytes.Buffer
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		if err := flags.run(ctx, cmd, st, peer); err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
		var count int
		var last int64
		for {
			var record struct {
				Seq int64 `json:"seq"`
			}
			err := decoder.Decode(&record)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			count++
			last = record.Seq
		}
		if count < 500 && stdout.Len() < 2000000 {
			return nil
		}
		flags.rf.sinceSeq = last
	}
}
