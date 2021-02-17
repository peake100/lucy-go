package lucymongo

import (
	"context"
	"errors"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/protoBson"
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"github.com/peake100/lucy-go/internal/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
)

const EnvKeyMongoURI = "LUCY_MONGO_URI"
const EnvKeyDBName = "LUCY_DB_NAME"

// Backend holds our connection information and methods for mongoDB and implements
// db.Connection.
type Backend struct {
	Client  *mongo.Client
	DB      *mongo.Database
	Jobs    *mongo.Collection
	ErrsGen *pkerr.ErrorGenerator
}

// CheckMongoErr takes in a mongodb error and transforms it into a pkerr.APIError.
func (backend Backend) CheckMongoErr(err error, message string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return backend.ErrsGen.NewErr(
			pkerr.ErrNotFound,
			message,
			nil,
			err,
		)
	}
	return fmt.Errorf("error finding record: %w", err)
}

// New returns a Backend value with our mongo client and collections.
func New(
	ctx context.Context, errGen *pkerr.ErrorGenerator,
) (db.Connection, error) {
	// Get out mongodb client
	mongoURI := os.Getenv(EnvKeyMongoURI)
	if mongoURI == "" {
		mongoURI = "mongodb://127.0.0.1:27017"
	}

	// protoCereal offers a number of bson codecs for automatic serialization of UUID
	// and other types.

	clientOpts := options.Client().
		ApplyURI(mongoURI).
		SetRegistry(BsonRegistry)

	result := Backend{}
	var err error
	result.Client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		return Backend{}, fmt.Errorf("error connecting mongo client: %w", err)
	}

	dbName := os.Getenv(EnvKeyDBName)
	if dbName == "" {
		dbName = "lucy"
	}
	result.DB = result.Client.Database(dbName)
	result.Jobs = result.DB.Collection("jobs")
	result.ErrsGen = errGen

	return result, nil
}

// BsonRegistry is our bson codec registry for marshalling and unmarshalling
var BsonRegistry = func() *bsoncodec.Registry {
	bsonRegistryBuilder := bson.NewRegistryBuilder()
	err := protoBson.RegisterCerealCodecs(
		bsonRegistryBuilder, protoBson.NewMongoOpts(),
	)
	if err != nil {
		panic(fmt.Errorf("error building bson registry: %w", err))
	}
	return bsonRegistryBuilder.Build()
}()
