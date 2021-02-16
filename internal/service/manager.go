package service

import (
	"github.com/peake100/gRPEAKEC-go/pkservices"
	"github.com/rs/zerolog"
	"os"
)

func NewLucyManager() (*pkservices.Manager, zerolog.Logger) {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).
		With().
		Timestamp().
		Logger()

	lucy := NewLucy()
	opts := pkservices.NewManagerOpts().
		WithErrorGenerator(lucy.errs).
		WithErrNotFoundMiddleware().
		WithLogger(logger).
		WithGrpcLogging(
			zerolog.DebugLevel,
			zerolog.DebugLevel,
			zerolog.DebugLevel,
			true,
			true,
		)

	return pkservices.NewManager(opts, lucy), logger
}
