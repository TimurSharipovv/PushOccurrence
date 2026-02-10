package mongoDb

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (r *outboxRepository) FetchPending(ctx context.Context, limit int) ([]OutboxMessage, error) {
	filter := bson.M{
		"status": "pending",
	}

	opts := options.Find().SetSort(bson.M{"created_at": 1}).SetLimit(int64(limit))

	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result []OutboxMessage
	if err := cursor.All(ctx, &result); err != nil {
		return nil, err
	}

	return result, nil
}
