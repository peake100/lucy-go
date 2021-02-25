package service

import (
	"context"
	"fmt"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/internal/db/lucymongo"
	"github.com/peake100/lucy-go/internal/db/lucysql"
	"github.com/peake100/lucy-go/internal/messaging"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"os"
	"strings"
	"sync"
)

// Lucy implements lucy.LucyServer and pkservices.GrpcService
type Lucy struct {
	// db holds the db connection for the service.
	db db.Backend
	// messenger is used to queue jobs and fre events.
	messenger messaging.LucyMessenger
	// errs is the error generator for lucy.
	errs *pkerr.ErrorGenerator
}

// Id implements pkservices.Service and returns Lucy.
func (Lucy) Id() string {
	return "Lucy"
}

// revive:disable:context-as-argument

// Setup implements pkservices.Service and sets up our dbMongo and rabbitMQ connections.
func (service *Lucy) Setup(
	resourcesCtx context.Context,
	resourcesReleased *sync.WaitGroup,
	shutdownCtx context.Context,
	logger zerolog.Logger,
) (err error) {
	service.db, err = newDBBackend(resourcesCtx, service.errs)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}

	resourcesReleased.Add(1)
	go func() {
		defer resourcesReleased.Done()
		defer service.db.Disconnect(shutdownCtx)
		<-resourcesCtx.Done()
	}()

	service.messenger = messaging.NewMessenger()
	err = service.messenger.Setup(resourcesCtx, logger)
	if err != nil {
		return fmt.Errorf("error setting up messenger: %w", err)
	}

	resourcesReleased.Add(1)
	go func() {
		defer resourcesReleased.Done()
		_ = service.messenger.Run(resourcesCtx)
	}()

	return nil
}

// backendTypesList is a list of valid backend type values for use in error messages.
var backendTypesList = fmt.Sprintf(
	"'%v'",
	strings.Join(
		[]string{db.BackendTypeMongo, db.BackendTypeSQLite},
		"', '",
	),
)

// newDBBackend selects a backend based on environmental variable configuration, then
// initializes it.
func newDBBackend(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (db.Backend, error) {
	backendType := db.BackendType(os.Getenv(db.EnvKeyBackendType))
	if backendType == "" {
		return nil, fmt.Errorf(
			"backend type is not configured, please set env var '%v' to "+
				"one of the following: %v",
			db.EnvKeyBackendType,
			backendTypesList,
		)
	}

	var newFunc db.NewBackendFunc

	switch backendType {
	case db.BackendTypeMongo:
		newFunc = lucymongo.New
	case db.BackendTypeSQLite:
		newFunc = lucysql.New
	default:
		return nil, fmt.Errorf(
			"backend type '%v' not recognized. available options: %v",
			backendType,
			backendTypesList,
		)
	}

	return newFunc(ctx, errGen)
}

// revive:enable:context-as-argument

// RegisterOnServer implements pkservices.GrpcService.
func (service *Lucy) RegisterOnServer(server *grpc.Server) {
	lucy.RegisterLucyServer(server, service)
}

// NewLucy returns a new Lucy service.
func NewLucy() *Lucy {
	return &Lucy{
		errs: pkerr.NewErrGenerator(
			"Lucy",
			true,
			true,
			true,
			true,
		),
	}
}
