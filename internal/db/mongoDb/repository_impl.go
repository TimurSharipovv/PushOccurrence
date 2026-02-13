package mongoDb

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type OutboxRepository struct {
	collection *mongo.Collection
}

func NewOutboxRepository(db *mongo.Database) OutboxRepositoryInteface {
	return &OutboxRepository{collection: db.Collection("messages")}
}
