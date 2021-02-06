package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/service/lucydb"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"sync"
)

// Lucy implements lucy.LucyServer and pkservices.GrpcService
type Lucy struct {
	// db holds the mongo client and collections for lucy.
	db lucydb.LucyDB
	// errs is the error generator for lucy.
	errs *pkerr.ErrorGenerator
}

// Id implements pkservices.Service and returns Lucy.
func (Lucy) Id() string {
	return "Lucy"
}

// revive:disable:context-as-argument

// Setup implements pkservices.Service and sets up our db and rabbitMQ connections.
func (service *Lucy) Setup(
	resourcesCtx context.Context,
	resourcesReleased *sync.WaitGroup,
	shutdownCtx context.Context,
	logger zerolog.Logger,
) (err error) {
	service.db, err = lucydb.Connect(resourcesCtx)
	if err != nil {
		return err
	}

	resourcesReleased.Add(1)
	go func() {
		defer resourcesReleased.Done()
		defer service.db.Client.Disconnect(shutdownCtx)
		<-resourcesCtx.Done()
	}()

	return nil
}

// revive:enable:context-as-argument

// RegisterOnServer implements pkservices.GrpcService.
func (service *Lucy) RegisterOnServer(server *grpc.Server) {
	lucy.RegisterLucyServer(server, service)
}

// CheckMongoErr takes in a mongodb error and transforms it into a pkerr.APIError.
func (service *Lucy) CheckMongoErr(err error, message string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return service.errs.NewErr(
			pkerr.ErrNotFound,
			message,
			nil,
			err,
		)
	}
	return fmt.Errorf("error finding record: %w", err)
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
