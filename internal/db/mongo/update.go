package mongoDb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (r *OutboxRepository) MarkProcessing(ctx context.Context, messageId string) error {
	oid, err := primitive.ObjectIDFromHex(messageId)
	if err != nil {
		return err
	}

	filter := bson.M{"_id": oid}
	update := bson.M{
		"$set": bson.M{
			"status":    "processing",
			"updatedAt": time.Now(),
		},
	}

	_, err = r.collection.UpdateOne(ctx, filter, update)
	return err
}

func (r *OutboxRepository) MarkSent(ctx context.Context, messageId string) error {
	oid, err := primitive.ObjectIDFromHex(messageId)
	if err != nil {
		return err
	}

	filter := bson.M{"_id": oid}
	update := bson.M{
		"$set": bson.M{
			"status":    "sent",
			"updatedAt": time.Now(),
		},
	}

	_, err = r.collection.UpdateOne(ctx, filter, update)
	return err
}

func (r *OutboxRepository) MarkFailed(ctx context.Context, messageId string, errMsg string) error {
	oid, err := primitive.ObjectIDFromHex(messageId)
	if err != nil {
		return err
	}

	filter := bson.M{"_id": oid}
	update := bson.M{
		"$set": bson.M{
			"status":     "failed",
			"updatedAt":  time.Now(),
			"last_error": errMsg,
		},
		"$inc": bson.M{
			"attempts": 1,
		},
	}

	_, err = r.collection.UpdateOne(ctx, filter, update)
	return err
}
