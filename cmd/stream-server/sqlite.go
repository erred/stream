package main

import (
	"context"
	"database/sql"
	"sync"
)

type SQLite struct {
	db *sql.DB

	once map[Table]*sync.Once
	stmt map[Table]*sql.Stmt
}

func NewSQLite(dsn string) (*SQLite, error) {
	s := &SQLite{
		once: make(map[Table]*sync.Once),
		stmt: make(map[Table]*sql.Stmt),
	}
	for _, t := range sqliteTables {
		s.once[t] = &sync.Once{}
	}

	var err error
	s.db, err = sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	err = s.db.Ping()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SQLite) setup(ctx context.Context, table Table) error {
	var err error
	s.once[table].Do(func() {
		_, err := s.db.ExecContext(ctx, sqliteTable[table])
		if err != nil {
			return
		}
		s.stmt[table], err = s.db.PrepareContext(ctx, sqlitePrep[table])
		if err != nil {
			return

		}
	})
	return err
}

type Table int

const (
	tableHTTP Table = iota
	tableCSP
	tableBeacon
	tableRepo
)

var (
	sqliteTables = []Table{tableHTTP, tableCSP, tableBeacon, tableRepo}

	sqliteTable = map[Table]string{
		tableHTTP: `
CREATE TABLE IF NOT EXISTS repo (
	timestamp	TEXT,
	method		TEXT,
	domain		TEXT,
	path		TEXT,
	remote		TEXT,
	user_agent	TEXT,
	referrer	TEXT,
);`,
		tableCSP: `
CREATE TABLE IF NOT EXISTS repo (
	timestamp		TEXT,
	remote			TEXT,
	user_agent		TEXT,
	referrer		TEXT,
	enforce			TEXT;
	blocked_uri		TEXT,
	source_file		TEXT,
	document_uri		TEXT,
	violated_directive	TEXT,
	effective_directive	TEXT,
	line_number		INTEGER,
	status_code		INTEGER
);`,
		tableBeacon: `
CREATE TABLE IF NOT EXISTS beacon (
	duration_ms	INTEGER,
	src_page	TEXT,
	dst_page	TEXT,
	remote		TEXT,
	user_agent	TEXT,
	referrer	TEXT
);`,
		tableRepo: `
CREATE TABLE IF NOT EXISTS repo (
	timestamp	TEXT,
	owner		TEXT,
	repo		TEXT
);`,
	}

	sqlitePrep = map[Table]string{
		tableHTTP: `
INSERT INTO http (timestamp, method, domain, path, remote, user_agent, referrer)
VALUES (?, ?, ?, ?, ?, ?, ?);`,
		tableCSP: `
INSERT INTO http (timestamp, remote, user_agent, referrer, enforce, blocked_uri, source_file, document_uri, violated_directive, effective_directive, line_number, status_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		tableBeacon: `
INSERT INTO beacon (duration_ms, src_page, dst_page, remote, user_agent, referrer)
VALUES (?, ?, ?, ?,?, ?);`,
		tableRepo: `
INSERT INTO repo (timestamp, owner, repo)
VALUES (?, ?, ?);`,
	}
)
