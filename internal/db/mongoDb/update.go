package mongoDb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func (r *outboxRepository) MarkProcessing(ctx context.Context, messageId string) error {
	_, err := r.collection.UpdateOne(ctx, bson.M{"message_id": messageId}, bson.M{"$Set": bson.M{"status": "processing", "updated_at": time.Now()}})

	return err
}

func (r *outboxRepository) MarkSent(ctx context.Context, messageId string) error {
	_, err := r.collection.UpdateOne(ctx, bson.M{"message_id": messageId}, bson.M{"$Set": bson.M{"status": "sent", "updated_at": time.Now()}})

	return err
}

func (r *outboxRepository) MarkFailed(ctx context.Context, messageId string, errMsg string) error {
	_, err := r.collection.UpdateOne(ctx, bson.M{"messageId": messageId}, bson.M{"$Set": bson.M{"status": "failed", "updated_at": time.Now(), "last_error": errMsg}, "$inc": bson.M{"attempts": 1}})

	return err
}
