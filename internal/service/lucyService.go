package service

import (
	"context"
	"fmt"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"github.com/peake100/lucy-go/internal/db/lucymongo"
	"github.com/peake100/lucy-go/internal/messaging"
	"github.com/peake100/lucy-go/pkg/lucy"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"sync"
)

// Lucy implements lucy.LucyServer and pkservices.GrpcService
type Lucy struct {
	// db holds the db connection for the service.
	db db.Backend
	// dbMongo holds the mongo client and collections for lucy.
	dbMongo lucymongo.Backend
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
	service.db, err = lucymongo.New(resourcesCtx, service.errs)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}

	service.dbMongo = service.db.(lucymongo.Backend)

	resourcesReleased.Add(1)
	go func() {
		defer resourcesReleased.Done()
		defer service.dbMongo.Client.Disconnect(shutdownCtx)
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
