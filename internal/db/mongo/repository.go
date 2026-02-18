package mongoDb

import (
	"context"
)

type OutboxRepositoryInteface interface {
	Insert(ctx context.Context, msg OutboxMessage) (string, error)
	FetchPending(ctx context.Context, limit int) ([]OutboxMessage, error)
	MarkProcessing(ctx context.Context, messageId string) error
	MarkSent(ctx context.Context, messageId string) error
	MarkFailed(ctx context.Context, messageId string, errMsg string) error
}
