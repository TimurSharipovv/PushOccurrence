package mongoDb

import (
	"context"
)

func (r *OutboxRepository) Insert(ctx context.Context, msg OutboxMessage) error {
	_, err := r.collection.InsertOne(ctx, msg)
	return err
}
