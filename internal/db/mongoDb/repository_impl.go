package mongoDb

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type outboxRepository struct {
	collection *mongo.Collection
}

func NewOutboxRepository(db *mongo.Database) OutboxRepository {
	return &outboxRepository{collection: db.Collection("messages")}
}
