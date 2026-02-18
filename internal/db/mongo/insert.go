package mongoDb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (r *OutboxRepository) Insert(ctx context.Context, msg OutboxMessage) (string, error) {
	now := time.Now()

	msg.Status = "pending"
	msg.Attempts = 0
	msg.CreatedAt = now
	msg.UpdatedAt = now
	res, err := r.collection.InsertOne(ctx, msg)
	if err != nil {
		return "", err
	}

	return res.InsertedID.(primitive.ObjectID).Hex(), nil
}
