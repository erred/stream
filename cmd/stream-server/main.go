package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/rs/zerolog"
	"go.seankhliao.com/stream"
	"go.seankhliao.com/usvc"

	_ "modernc.org/sqlite"
)

var (
	exampleSQLiteDSN = `file:test.db?cache=shared&mode=memory`
	// exampleCockroachDBDSN = `postgresql://root@cockroachdb:26257/?ssl=true&sslmode=require&sslrootcert=/var/secret/certs/ca.crt&sslkey=/var/secret/certs/tls.key&sslcert=/var/secret/certs/tls.crt`
)

func main() {
	var s Server

	srvc := usvc.DefaultConf(&s)
	s.log = srvc.Logger()

	ctx := context.Background()
	err := s.setup(ctx)
	if err != nil {
		s.log.Fatal().Err(err).Msg("setup database")
	}

	_, grpcServer, run, err := srvc.Server(nil)
	if err != nil {
		s.log.Fatal().Err(err).Msg("setup server")
	}

	stream.RegisterStreamService(grpcServer, &stream.StreamService{
		LogHTTP:   s.LogHTTP,
		LogCSP:    s.LogCSP,
		LogBeacon: s.LogBeacon,
		LogRepo:   s.LogRepo,
	})

	err = run(ctx)
	if err != nil {
		s.log.Fatal().Err(err).Msg("run server")
	}
}

type Server struct {
	sqliteDSN string
	// cockroachDSN string
	// crPool       *pgxpool.Pool

	sqlite *SQLite

	log zerolog.Logger
}

func (s *Server) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&s.sqliteDSN, "sqlite", "", exampleSQLiteDSN)
	// fs.StringVar(&s.cockroachDSN, "cockroachdb", "", exampleCockroachDBDSN)
}

func (s *Server) setup(ctx context.Context) error {
	var err error
	if s.sqliteDSN != "" {
		s.sqlite, err = NewSQLite(s.sqliteDSN)
		if err != nil {
			return err
		}
	}
	// if s.cockroachDSN != "" {
	// 	s.crPool, err = pgxpool.Connect(ctx, s.cockroachDSN)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	c, err := s.crPool.Acquire(ctx)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	err = c.Conn().Ping(ctx)
	// 	c.Release()
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

func (s *Server) LogHTTP(ctx context.Context, r *stream.HTTPRequest) (*stream.Result, error) {
	err := s.sqlite.setup(ctx, tableHTTP)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("http db init: %w", err)
	}

	_, err = s.sqlite.stmt[tableHTTP].Exec(r.Timestamp, r.Method, r.Domain, r.Path, r.Remote, r.UserAgent, r.Referrer)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("http db insert: %w", err)
	}
	return &stream.Result{}, nil
}
func (s *Server) LogBeacon(ctx context.Context, r *stream.BeaconRequest) (*stream.Result, error) {
	err := s.sqlite.setup(ctx, tableBeacon)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("beacon db init: %w", err)
	}

	_, err = s.sqlite.stmt[tableBeacon].Exec(r.DurationMs, r.SrcPage, r.DstPage, r.Remote, r.UserAgent, r.Referrer)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("beacon db insert: %w", err)
	}
	return &stream.Result{}, nil
}
func (s *Server) LogCSP(ctx context.Context, r *stream.CSPRequest) (*stream.Result, error) {
	err := s.sqlite.setup(ctx, tableCSP)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("csp db init: %w", err)
	}

	_, err = s.sqlite.stmt[tableCSP].Exec(r.Timestamp, r.Remote, r.UserAgent, r.Referrer, r.Enforce, r.BlockedUri, r.SourceFile, r.DocumentUri, r.ViolatedDirective, r.EffectiveDirective, r.LineNumber, r.StatusCode)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("csp db insert: %w", err)
	}
	return &stream.Result{}, nil
}

func (s *Server) LogRepo(ctx context.Context, r *stream.RepoRequest) (*stream.Result, error) {
	err := s.sqlite.setup(ctx, tableRepo)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("repo db init: %w", err)
	}

	_, err = s.sqlite.stmt[tableRepo].Exec(r.Timestamp, r.Owner, r.Repo)
	if err != nil {
		// TODO: log?
		return nil, fmt.Errorf("repo db insert: %w", err)
	}
	return &stream.Result{}, nil
}
