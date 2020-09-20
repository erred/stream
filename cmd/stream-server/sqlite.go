package main

import (
	"context"
	"database/sql"
)

type SQLite struct {
	db *sql.DB

	stmt map[Table]*sql.Stmt
}

func NewSQLite(dsn string) (*SQLite, error) {
	s := &SQLite{
		stmt: make(map[Table]*sql.Stmt),
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
	for _, t := range sqliteTables {
		_, err := s.db.ExecContext(context.Background(), sqliteTable[t])
		if err != nil {
			return nil, err
		}
		s.stmt[t], err = s.db.PrepareContext(context.Background(), sqlitePrep[t])
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *SQLite) insert(ctx context.Context, table Table, args ...interface{}) error {
	_, err := s.stmt[tableRepo].Exec(args...)
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
