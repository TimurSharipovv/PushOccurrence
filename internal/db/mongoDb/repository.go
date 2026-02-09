package mongoDb

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type OutboxRepository struct {
	collection *mongo.Collection
}

func NewOutboxRepository(db *mongo.Database) *OutboxRepository {
	return &OutboxRepository{
		collection: db.Collection("messages"),
	}
}
