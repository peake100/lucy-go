package lucymongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/protobson"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"testing"
)

// Backend holds our connection information and methods for mongoDB and implements
// db.Backend.
type Backend struct {
	client  *mongo.Client
	db      *mongo.Database
	jobs    *mongo.Collection
	errsGen *pkerr.ErrorGenerator
}

// Disconnect releases the underlying mongo connection.
func (backend Backend) Disconnect(ctx context.Context) error {
	return backend.client.Disconnect(ctx)
}

// CheckMongoErr takes in a mongodb error and transforms it into a pkerr.APIError.
func (backend Backend) CheckMongoErr(err error, message string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return backend.errsGen.NewErr(
			pkerr.ErrNotFound,
			message,
			nil,
			err,
		)
	}
	return fmt.Errorf("error finding record: %w", err)
}

// Tester returns a testing harness with methods designed for testing.
func (backend Backend) Tester(t *testing.T) BackendTester {
	return BackendTester{
		t:       t,
		backend: backend,
	}
}

// BackendTester contains testing methods for testing our backend.
type BackendTester struct {
	t       *testing.T
	backend Backend
}

// DB returns the underlying database connection
func (tester BackendTester) DB() *mongo.Database {
	return tester.backend.db
}

// New returns a Backend value with our mongo client and collections.
func New(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (db.Backend, error) {
	// Get out mongodb client
	mongoURI := os.Getenv(EnvKeyURI)
	if mongoURI == "" {
		return nil, fmt.Errorf(
			"no mongo URI specified. please set '%v' env var with uri of mongo"+
				" database",
			EnvKeyURI,
		)
	}

	// protoCereal offers a number of bson codecs for automatic serialization of UUID
	// and other types.

	clientOpts := options.Client().
		ApplyURI(mongoURI).
		SetRegistry(BsonRegistry)

	result := Backend{}
	var err error
	result.client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		return Backend{}, fmt.Errorf("error connecting mongo client: %w", err)
	}

	dbName := os.Getenv(EnvKeyDBName)
	if dbName == "" {
		return nil, fmt.Errorf(
			"no mongo db name specified. please set '%v' env var with name of"+
				" target database",
			EnvKeyDBName,
		)
	}
	result.db = result.client.Database(dbName)
	result.jobs = result.db.Collection("jobs")
	result.errsGen = errGen

	return result, nil
}

// BsonRegistry is our bson codec registry for marshalling and unmarshalling
var BsonRegistry = func() *bsoncodec.Registry {
	bsonRegistryBuilder := bson.NewRegistryBuilder()
	err := protobson.RegisterCerealCodecs(
		bsonRegistryBuilder, protobson.NewMongoOpts(),
	)
	if err != nil {
		panic(fmt.Errorf("error building bson registry: %w", err))
	}
	return bsonRegistryBuilder.Build()
}()
