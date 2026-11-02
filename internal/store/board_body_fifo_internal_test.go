package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestBoardNonregularReviewBodiesUseExactRecordText(t *testing.T) {
	for _, kind := range []kinds.Kind{kinds.Head, kinds.Review} {
		for _, object := range []string{"fifo", "directory"} {
			t.Run(string(kind)+"/"+object, func(t *testing.T) {
				st, index, spec, _ := boardFixture(t)
				path := filepath.Join(spec.CWD, "review-body")
				if object == "fifo" {
					if err := syscall.Mkfifo(path, 0o600); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				text := string(kind) + " #123 exact fallback …"
				record, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "reviewer", Kind: kind, Text: text, Refs: Refs{Path: path}}, 0)
				if err != nil {
					t.Fatal(err)
				}
				boardFIFOOperationReturns(t, path, func() error {
					_, err := index.Provision(t.Context(), spec)
					return err
				})
				read := func() error {
					return index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
						reviews := result.Reviews
						if reviews.Error != nil {
							return reviews.Error
						}
						if len(reviews.Records) != 1 || reviews.Records[0].Seq != record.Seq || reviews.Records[0].Text != text {
							return fmt.Errorf("review text fallback changed: %+v", reviews.Records)
						}
						if reviews.BodyFiles[record.Seq] || len(reviews.Bodies[record.Seq]) != 0 {
							return fmt.Errorf("nonregular review body captured: regular=%v body=%q", reviews.BodyFiles[record.Seq], reviews.Bodies[record.Seq])
						}
						return nil
					})
				}
				boardFIFOOperationReturns(t, path, read)
				boardFIFOOperationReturns(t, path, read)
			})
		}
	}
}

func TestReadBoardBodyRegularReplacedByFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, []byte("regular body"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatal("replacement control did not begin with a regular file")
	}
	replacement := path + ".fifo"
	if err := syscall.Mkfifo(replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	var body []byte
	var regular bool
	var readErr error
	boardFIFOOperationReturns(t, path, func() error {
		body, regular, readErr = readBoardBody(path, info.Mode().IsRegular())
		return nil
	})
	if regular || len(body) != 0 || !errors.Is(readErr, syscall.EINVAL) {
		t.Fatalf("replacement FIFO accepted: regular=%v body=%q error=%v", regular, body, readErr)
	}
	var pathErr *os.PathError
	if !errors.As(readErr, &pathErr) || pathErr.Op != "read" || pathErr.Path != path {
		t.Fatalf("replacement FIFO error lost its path: %v", readErr)
	}
}

func TestReadBoardBodyPreservesRegularMissingDirectoryErrors(t *testing.T) {
	for _, object := range []string{"regular", "symlink", "missing", "directory", "not-directory"} {
		t.Run(object, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "body")
			switch object {
			case "regular", "symlink", "not-directory":
				if err := os.WriteFile(path, []byte("raw\r\nbody\x00\xff"), 0o600); err != nil {
					t.Fatal(err)
				}
				if object == "symlink" {
					link := path + ".link"
					if err := os.Symlink(path, link); err != nil {
						t.Fatal(err)
					}
					path = link
				}
				if object == "not-directory" {
					path = filepath.Join(path, "child")
				}
			case "directory":
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			info, statErr := os.Stat(path)
			before := statErr == nil && info.Mode().IsRegular()
			want, wantErr := os.ReadFile(path)
			got, regular, err := readBoardBody(path, before)
			if regular != before || !bytes.Equal(got, want) {
				t.Fatalf("body changed: regular=%v want=%v body=%q want=%q", regular, before, got, want)
			}
			if (err == nil) != (wantErr == nil) {
				t.Fatalf("read error changed: got=%v want=%v", err, wantErr)
			}
			if err != nil && err.Error() != wantErr.Error() {
				t.Fatalf("read error text changed: got=%q want=%q", err, wantErr)
			}
		})
	}
}

func TestReadBoardBodyDoesNotPromoteMissingPreStat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "body")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control did not begin missing: %v", err)
	}
	want := []byte("created after stat")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	body, regular, err := readBoardBody(path, false)
	if err != nil || regular || !bytes.Equal(body, want) {
		t.Fatalf("missing-to-regular semantics changed: body=%q regular=%v error=%v", body, regular, err)
	}
}

func boardFIFOOperationReturns(t *testing.T, path string, operation func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- operation() }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		return
	case <-timer.C:
	}
	ticks := time.NewTicker(time.Millisecond)
	defer ticks.Stop()
	for {
		select {
		case err := <-done:
			t.Fatalf("BOARD_REVIEW_FIFO_BLOCKED: operation exceeded five seconds before returning: %v", err)
		default:
		}
		writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			closeErr := writer.Close()
			operationErr := <-done
			t.Fatalf("BOARD_REVIEW_FIFO_BLOCKED: operation required a FIFO writer to return: operation=%v close=%v", operationErr, closeErr)
		}
		if !errors.Is(err, syscall.ENXIO) {
			panic(fmt.Errorf("open owned FIFO cleanup writer: %w", err))
		}
		<-ticks.C
	}
}
