// Package github turns GitHub repository webhooks, which GitHub signs with
// X-Hub-Signature-256, into one github record per pull request, review or
// check change, or push to the default branch.
package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/service"
)

var ErrSignature = errors.New("github: webhook signature does not verify")

// Verify checks GitHub's "sha256=<hex>" HMAC-SHA256 of body under secret.
func Verify(secret string, header http.Header, body []byte) error {
	sig, found := strings.CutPrefix(header.Get("X-Hub-Signature-256"), "sha256=")
	got, err := hex.DecodeString(sig)
	if !found || err != nil {
		return ErrSignature
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return ErrSignature
	}
	return nil
}

type repository struct {
	FullName string `json:"full_name"`
}

type user struct {
	Login string `json:"login"`
}

type pullRequestEvent struct {
	Action string `json:"action"`
	Number int    `json:"number"`
	Before string `json:"before"`
	Label  *struct {
		Name string `json:"name"`
	} `json:"label"`
	PullRequest struct {
		HTMLURL   string    `json:"html_url"`
		Draft     bool      `json:"draft"`
		Merged    bool      `json:"merged"`
		MergeSHA  string    `json:"merge_commit_sha"`
		UpdatedAt time.Time `json:"updated_at"`
		User      user      `json:"user"`
		Labels    []struct {
			Name string `json:"name"`
		} `json:"labels"`
		Head struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"pull_request"`
	Repository repository `json:"repository"`
}

type reviewEvent struct {
	Action string `json:"action"`
	Review struct {
		State       string    `json:"state"`
		CommitID    string    `json:"commit_id"`
		HTMLURL     string    `json:"html_url"`
		SubmittedAt time.Time `json:"submitted_at"`
		User        user      `json:"user"`
	} `json:"review"`
	PullRequest struct {
		Number    int       `json:"number"`
		UpdatedAt time.Time `json:"updated_at"`
	} `json:"pull_request"`
	Repository repository `json:"repository"`
}

type checkRunEvent struct {
	Action   string `json:"action"`
	CheckRun struct {
		ID           int64     `json:"id"`
		Name         string    `json:"name"`
		HeadSHA      string    `json:"head_sha"`
		Conclusion   string    `json:"conclusion"`
		DetailsURL   string    `json:"details_url"`
		CompletedAt  time.Time `json:"completed_at"`
		PullRequests []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
	} `json:"check_run"`
	Repository repository `json:"repository"`
}

type statusEvent struct {
	ID         int64      `json:"id"`
	SHA        string     `json:"sha"`
	Context    string     `json:"context"`
	State      string     `json:"state"`
	TargetURL  string     `json:"target_url"`
	UpdatedAt  time.Time  `json:"updated_at"`
	Repository repository `json:"repository"`
}

type pushEvent struct {
	Ref        string `json:"ref"`
	Before     string `json:"before"`
	After      string `json:"after"`
	Compare    string `json:"compare"`
	Repository struct {
		FullName      string `json:"full_name"`
		DefaultBranch string `json:"default_branch"`
		PushedAt      int64  `json:"pushed_at"`
	} `json:"repository"`
}

var pullRequestActions = []string{"opened", "reopened", "synchronize", "closed", "edited", "ready_for_review", "converted_to_draft", "labeled", "unlabeled"}

// Record is the github record for delivery id of an event named by the
// X-GitHub-Event header, false for an event or action no reader acts on.
// A status or a push names a commit, not a pull request, so its record carries
// no PR, and only a push to the default branch makes one.
func Record(event, id, drive string, body []byte) (service.Record, bool, error) {
	r := service.Record{ID: "github:" + id, Drive: drive, Kind: "github"}
	switch event {
	case "pull_request":
		var e pullRequestEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return r, false, err
		}
		if !slices.Contains(pullRequestActions, e.Action) {
			return r, false, nil
		}
		pr, outcome := ref(e.Repository, e.Number), e.Action
		if e.Action == "closed" && e.PullRequest.Merged {
			outcome = "merged"
		}
		r.At, r.PRs, r.URL, r.Outcome, r.Commit = e.PullRequest.UpdatedAt, []string{pr}, e.PullRequest.HTMLURL, outcome, e.PullRequest.Head.SHA
		r.Topic = "pull_request"
		labels := make([]string, 0, len(e.PullRequest.Labels))
		for _, l := range e.PullRequest.Labels {
			labels = append(labels, l.Name)
		}
		r.Attrs = map[string]string{
			"author": e.PullRequest.User.Login, "base": e.PullRequest.Base.Ref, "branch": e.PullRequest.Head.Ref,
			"draft": fmt.Sprint(e.PullRequest.Draft), "labels": strings.Join(labels, ","),
		}
		r.Text = fmt.Sprintf("%s %s at %s onto %s", pr, outcome, short(e.PullRequest.Head.SHA), e.PullRequest.Base.Ref)
		if outcome == "merged" {
			r.Attrs["merge"] = e.PullRequest.MergeSHA
		}
		if e.Before != "" {
			r.Attrs["before"] = e.Before
		}
		if e.Label != nil {
			r.Attrs["label"] = e.Label.Name
			r.Text += ": " + e.Label.Name
		}
		return r, true, nil
	case "pull_request_review":
		var e reviewEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return r, false, err
		}
		if e.Action != "submitted" && e.Action != "dismissed" {
			return r, false, nil
		}
		pr, state := ref(e.Repository, e.PullRequest.Number), strings.ToLower(e.Review.State)
		r.At, r.PRs, r.URL, r.Outcome, r.Commit = e.Review.SubmittedAt, []string{pr}, e.Review.HTMLURL, state, e.Review.CommitID
		if e.Action == "dismissed" {
			r.At = e.PullRequest.UpdatedAt
		}
		r.Topic = "review"
		r.Attrs = map[string]string{"reviewer": e.Review.User.Login}
		r.Text = fmt.Sprintf("%s review %s by %s at %s", pr, state, e.Review.User.Login, short(e.Review.CommitID))
		return r, true, nil
	case "check_run":
		var e checkRunEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return r, false, err
		}
		if e.Action != "completed" {
			return r, false, nil
		}
		c := e.CheckRun
		for _, pr := range c.PullRequests {
			r.PRs = append(r.PRs, ref(e.Repository, pr.Number))
		}
		r.At, r.URL, r.Outcome, r.Commit = c.CompletedAt, c.DetailsURL, c.Conclusion, c.HeadSHA
		r.Topic = "check"
		r.Attrs = map[string]string{"check": c.Name, "id": fmt.Sprint(c.ID)}
		r.Text = fmt.Sprintf("%s %s %s %s", e.Repository.FullName, short(c.HeadSHA), c.Name, c.Conclusion)
		return r, true, nil
	case "status":
		var e statusEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return r, false, err
		}
		if e.State == "pending" {
			return r, false, nil
		}
		r.At, r.URL, r.Outcome, r.Commit = e.UpdatedAt, e.TargetURL, e.State, e.SHA
		r.Topic = "check"
		r.Attrs = map[string]string{"check": e.Context, "id": fmt.Sprint(e.ID), "repo": e.Repository.FullName}
		r.Text = fmt.Sprintf("%s %s %s %s", e.Repository.FullName, short(e.SHA), e.Context, e.State)
		return r, true, nil
	case "push":
		var e pushEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return r, false, err
		}
		branch, _ := strings.CutPrefix(e.Ref, "refs/heads/")
		if branch != e.Repository.DefaultBranch {
			return r, false, nil
		}
		r.At, r.URL, r.Outcome, r.Commit = time.Unix(e.Repository.PushedAt, 0), e.Compare, "pushed", e.After
		r.Topic = "push"
		r.Attrs = map[string]string{"before": e.Before, "branch": branch, "repo": e.Repository.FullName}
		r.Text = fmt.Sprintf("%s %s pushed %s over %s", e.Repository.FullName, branch, short(e.After), short(e.Before))
		return r, true, nil
	}
	return r, false, nil
}

func ref(repo repository, number int) string {
	return fmt.Sprintf("%s#%d", repo.FullName, number)
}

func short(sha string) string {
	return sha[:min(10, len(sha))]
}

// Handler is the webhook endpoint: it verifies each delivery and posts its
// record to Drive through Writer.
type Handler struct {
	Secret string
	Writer service.Writer
	Drive  string
}

// Handle answers one delivery with the status GitHub reads: 2xx settles it,
// and any other code marks it failed for a redelivery.
func (h Handler) Handle(ctx context.Context, header http.Header, body []byte) int {
	if err := Verify(h.Secret, header, body); err != nil {
		slog.Warn("github webhook refused", "err", err)
		return http.StatusUnauthorized
	}
	event, id := header.Get("X-GitHub-Event"), header.Get("X-GitHub-Delivery")
	r, ok, err := Record(event, id, h.Drive, body)
	if err != nil {
		slog.Warn("github webhook undecodable", "event", event, "err", err)
		return http.StatusBadRequest
	}
	if !ok {
		return http.StatusNoContent
	}
	if err := h.Writer.Post(ctx, r); err != nil {
		slog.Error("github webhook not relayed", "event", event, "delivery", id, "err", err)
		return http.StatusBadGateway
	}
	slog.Info("github webhook relayed", "event", event, "delivery", id)
	return http.StatusOK
}
