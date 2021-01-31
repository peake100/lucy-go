package lucydb

import (
	"context"
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/protoBson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
)

const EnvKeyMongoURI = "LUCY_MONGO_URI"
const EnvKeyDBName = "LUCY_DB_NAME"

// LucyDB holds our connection information.
type LucyDB struct {
	Client *mongo.Client
	DB     *mongo.Database
	Jobs   *mongo.Collection
}

// Connect returns a LucyDB value with our mongo client and collections.
func Connect(ctx context.Context) (result LucyDB, err error) {
	// Get out mongodb client
	mongoURI := os.Getenv(EnvKeyMongoURI)
	if mongoURI == "" {
		mongoURI = "mongodb://127.0.0.1:27017"
	}

	bsonRegistryBuilder := bson.NewRegistryBuilder()

	// protoCereal offers a number of bson codecs for automatic serialization of UUID
	// and other types.
	err = protoBson.RegisterCerealCodecs(bsonRegistryBuilder, protoBson.NewMongoOpts())
	if err != nil {
		return LucyDB{}, fmt.Errorf("error registering bson codecs: %w", err)
	}

	clientOpts := options.Client().
		ApplyURI(mongoURI).
		SetRegistry(bsonRegistryBuilder.Build())

	result.Client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		return LucyDB{}, fmt.Errorf("error connecting mongo client: %w", err)
	}

	dbName := os.Getenv(EnvKeyDBName)
	if dbName == "" {
		dbName = "lucy"
	}
	result.DB = result.Client.Database(dbName)
	result.Jobs = result.DB.Collection("jobs")

	return result, nil
}
