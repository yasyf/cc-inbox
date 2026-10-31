package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type IncidentProvisionReport struct {
	Drive                string             `json:"drive"`
	Records              int64              `json:"records"`
	Paths                int64              `json:"paths"`
	Elapsed              time.Duration      `json:"elapsed_ns"`
	AdmissionTransaction time.Duration      `json:"admission_transaction_ns"`
	Ready                bool               `json:"ready"`
	Statistics           IncidentIndexStats `json:"statistics"`
	MaxWritePhase        time.Duration      `json:"max_write_phase_ns"`
}

func (s *Store) ProvisionIncidentScope(ctx context.Context, drive string) (IncidentProvisionReport, error) {
	started := time.Now()
	report := IncidentProvisionReport{Drive: drive}
	if drive == "" {
		return report, ErrNoDrive
	}
	lock, err := os.OpenFile(filepath.Join(s.home, "init.lock"), os.O_RDWR, 0) //nolint:gosec // G304: explicit provisioning holds the fixed existing init lock in the selected CCI home.
	if err != nil {
		return report, err
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return report, err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	installed, err := incidentProjectionInstalled(ctx, s.db)
	if err != nil {
		return report, err
	}
	if !installed {
		return report, errors.New("incident projection is not provisioned; run maintain-indexes first")
	}
	index, err := OpenIncidentIndex(ctx, s)
	if err != nil {
		return report, err
	}
	defer func() { _ = index.Close() }()
	transactionStarted := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer func() { _ = tx.Rollback() }()
	var ready bool
	err = tx.QueryRowContext(ctx, "SELECT ready FROM incident_scopes WHERE drive=?", drive).Scan(&ready)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, err
	}
	if ready {
		return report, fmt.Errorf("incident scope %s is already provisioned", drive)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, "INSERT INTO incident_scopes(drive) VALUES(?)", drive); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO incident_versions(source_seq,revision) SELECT seq,1 FROM records WHERE drive=? AND kind='incident' ON CONFLICT(source_seq) DO UPDATE SET revision=revision+1", drive); err != nil {
			return report, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO incident_changes(source_seq,revision) SELECT r.seq,v.revision FROM records r JOIN incident_versions v ON v.source_seq=r.seq WHERE r.drive=? AND r.kind='incident' ON CONFLICT(source_seq) DO UPDATE SET revision=excluded.revision", drive); err != nil {
			return report, err
		}
	}
	if err := tx.Commit(); err != nil {
		return report, err
	}
	report.AdmissionTransaction = time.Since(transactionStarted)
	if err := index.reconcile(ctx); err != nil {
		return report, err
	}
	var unsupported bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM incident_records WHERE drive=? AND unsupported_path!=0)", drive).Scan(&unsupported); err != nil {
		return report, err
	}
	if unsupported {
		return report, errors.New("incident scope contains relative body paths")
	}
	var message string
	err = s.db.QueryRowContext(ctx, "SELECT decode_error FROM incident_records WHERE drive=? AND decode_error!='' ORDER BY seq DESC LIMIT 1", drive).Scan(&message)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, err
	}
	if message != "" {
		return report, errors.New(message)
	}
	if err := index.checkBodyErrors(ctx, s, drive, s.Now().UnixMilli()); err != nil {
		return report, err
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE incident_scopes SET ready=1 WHERE drive=?", drive); err != nil {
		return report, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM incident_records WHERE drive=?", drive).Scan(&report.Records); err != nil {
		return report, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT count(DISTINCT body_path) FROM incident_records WHERE drive=? AND body_path!=''", drive).Scan(&report.Paths); err != nil {
		return report, err
	}
	report.Elapsed, report.Ready = time.Since(started), true
	report.Statistics = index.Stats()
	report.MaxWritePhase = max(report.AdmissionTransaction, report.Statistics.MaxWritePhase)
	return report, nil
}
