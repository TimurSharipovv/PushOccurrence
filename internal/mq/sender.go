package mq

import (
	"context"

	mongoDb "PushOccurrence/internal/db/mongo"
)

func SendToOutbox(ctx context.Context, msg Message, repo mongoDb.OutboxRepositoryInteface) error {
	outboxMsg := mongoDb.OutboxMessage{
		Payload: msg.Payload,
		Topic:   "failed_rabbit_msg",
	}

	_, err := repo.Insert(ctx, outboxMsg)
	return err
}
