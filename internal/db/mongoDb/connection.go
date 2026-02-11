package mongoDb

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoClient *mongo.Client

func Init(ctx context.Context, uri string) {
	var err error
	MongoClient, err = Connect(ctx, uri)
	if err != nil {
		log.Fatalf("failed to initialize mongo: %v", err)
	}
}

func Close(ctx context.Context) {
	if MongoClient != nil {
		if err := MongoClient.Disconnect(ctx); err != nil {
			log.Printf("error closing mongo connection: %v", err)
		}
	}
}

func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongo: %w", err)
	}

	log.Println("successfully connected to mongo")

	return client, nil
}
