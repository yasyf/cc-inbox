package store

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"unicode"
)

var (
	boardPRReference       = regexp.MustCompile(`[#\[]([\p{Nd}]+)`)
	boardRowReference      = regexp.MustCompile(`merge-row-([\p{Nd}]+)(?:-[0-9a-f]{7,40})?`)
	boardLaneNumber        = regexp.MustCompile(`^(.+?)(?:-([\p{Nd}]+))?$`)
	boardWithdrawReference = regexp.MustCompile(`(?i)\bwithdraws?\b[^.;:\n#]*#([\p{Nd}]+)`)
)

func boardDecimal(text string) *big.Int {
	var digits strings.Builder
	for _, r := range text {
		value := -1
		for _, entry := range unicode.Nd.R16 {
			if r >= rune(entry.Lo) && r <= rune(entry.Hi) && (r-rune(entry.Lo))%rune(entry.Stride) == 0 {
				value = int((r-rune(entry.Lo))/rune(entry.Stride)) % 10
				break
			}
		}
		if value < 0 {
			for _, entry := range unicode.Nd.R32 {
				if int64(r) >= int64(entry.Lo) && int64(r) <= int64(entry.Hi) && (int64(r)-int64(entry.Lo))%int64(entry.Stride) == 0 {
					value = int(((int64(r) - int64(entry.Lo)) / int64(entry.Stride)) % 10)
					break
				}
			}
		}
		if value < 0 {
			return nil
		}
		digits.WriteByte(byte('0' + value))
	}
	value, ok := new(big.Int).SetString(digits.String(), 10)
	if !ok {
		return nil
	}
	return value
}

func boardDelegated(lane string, delegates []string) bool {
	named := boardLaneNumber.FindStringSubmatch(lane)
	for _, delegate := range delegates {
		if lane == delegate {
			return true
		}
		base := boardLaneNumber.FindStringSubmatch(delegate)
		if named == nil || base == nil || named[1] != base[1] || named[2] == "" {
			continue
		}
		number := big.NewInt(1)
		if base[2] != "" {
			number = boardDecimal(base[2])
		}
		later := boardDecimal(named[2])
		if number != nil && later != nil && later.Cmp(number) > 0 {
			return true
		}
	}
	return false
}

func boardPutKeys(ctx context.Context, tx *sql.Tx, scope *boardScope, category int, record Record, body []byte) error {
	keys := map[[2]string]bool{}
	add := func(axis, key string) { keys[[2]string{axis, key}] = true }
	refs := func(text string) {
		for _, match := range boardPRReference.FindAllStringSubmatch(text, -1) {
			add("pr", match[1])
		}
	}
	switch category {
	case 0:
		if !boardRootEligible(scope, record) {
			return nil
		}
		refs(record.Text)
		text := record.Text
		if len(body) != 0 {
			text = boardText(body)
		}
		refs(text)
		for _, match := range boardRowReference.FindAllStringSubmatch(text, -1) {
			add("row", match[0])
			add("pr", match[1])
		}
	case 1:
		if len(record.Refs.PRs) != 0 {
			for _, pr := range record.Refs.PRs {
				if pr.Repo == "" || strings.EqualFold(pr.Repo, scope.spec.Repo) {
					add("pr", fmt.Sprint(pr.Number))
				}
			}
		} else {
			refs(record.Text)
		}
	case 2:
		if record.Lane != "root" {
			return nil
		}
		if record.Resolves != 0 {
			add("close", fmt.Sprint(record.Resolves))
		}
		for _, seq := range record.AlsoResolves {
			add("close", fmt.Sprint(seq))
		}
		if string(record.Kind) == "withdraw" {
			if record.Re != 0 {
				add("close", fmt.Sprint(record.Re))
			}
			for _, match := range boardWithdrawReference.FindAllStringSubmatch(record.Text, -1) {
				if number := boardDecimal(match[1]); number != nil {
					add("close", number.String())
				}
			}
		}
	}
	for pair := range keys {
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO board_query_keys(scope,category,axis,key,seq) VALUES(?,?,?,?,?)", scope.id, category, pair[0], pair[1], record.Seq); err != nil {
			return err
		}
	}
	return nil
}
