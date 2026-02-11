package mongoDb

import (
	"context"
	"time"
)

func (r *outboxRepository) Insert(ctx context.Context, msg OutboxMessage) error {
	now := time.Now()

	msg.Status = "pending"
	msg.Attempts = 0
	msg.CreatedAt = now
	msg.UpdatedAt = now
	_, err := r.collection.InsertOne(ctx, msg)

	return err
}
