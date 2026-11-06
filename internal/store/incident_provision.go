package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	lock, err := lockMaintenance(s.home)
	if err != nil {
		return report, err
	}
	defer func() { _ = lock.Close() }()
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
	tx, err := s.rw.BeginTx(ctx, nil)
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
	}
	if err := tx.Commit(); err != nil {
		return report, err
	}
	report.AdmissionTransaction = time.Since(transactionStarted)
	for after := int64(0); ; {
		started := time.Now()
		last, err := s.admitIncidentPage(ctx, drive, after)
		report.AdmissionTransaction = max(report.AdmissionTransaction, time.Since(started))
		if err != nil {
			return report, err
		}
		if last == 0 {
			break
		}
		after = last
	}
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
	if _, err := s.rw.ExecContext(ctx, "UPDATE incident_scopes SET ready=1 WHERE drive=?", drive); err != nil {
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

func (s *Store) admitIncidentPage(ctx context.Context, drive string, after int64) (int64, error) {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT max(seq) FROM (SELECT seq FROM records WHERE drive=? AND kind='incident' AND seq>? ORDER BY seq LIMIT ?)", drive, after, WritePage).Scan(&last); err != nil {
		return 0, err
	}
	if !last.Valid {
		return 0, nil
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO incident_versions(source_seq,revision) SELECT seq,1 FROM records WHERE drive=? AND kind='incident' AND seq>? AND seq<=? ON CONFLICT(source_seq) DO UPDATE SET revision=revision+1", drive, after, last.Int64); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO incident_changes(source_seq,revision) SELECT r.seq,v.revision FROM records r JOIN incident_versions v ON v.source_seq=r.seq WHERE r.drive=? AND r.kind='incident' AND r.seq>? AND r.seq<=? ON CONFLICT(source_seq) DO UPDATE SET revision=excluded.revision", drive, after, last.Int64); err != nil {
		return 0, err
	}
	return last.Int64, tx.Commit()
}
