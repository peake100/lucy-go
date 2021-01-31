package service

import (
	"context"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/lucy"
	"github.com/peake100/lucy-go/service/lucydb"
	"github.com/rs/zerolog"
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

// RegisterOnServer implements pkservices.GrpcService.
func (service *Lucy) RegisterOnServer(server *grpc.Server) {
	lucy.RegisterLucyServer(server, service)
}

// NewLucy returns a new Lucy service.
func NewLucy() *Lucy {
	return new(Lucy)
}
