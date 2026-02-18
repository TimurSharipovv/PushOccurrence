package service

import (
	"context"
	"log"
	"time"

	mongoDb "PushOccurrence/internal/db/mongo"
	"PushOccurrence/internal/mq"
)

func StartPoller(ctx context.Context, repo mongoDb.OutboxRepositoryInteface, rabbit *mq.Mq) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			println("poller stopping")
			return
		case <-ticker.C:
			messages, err := repo.FetchPending(ctx, 50)
			if err != nil {
				log.Printf("error fetch message from mongo %v", err)
				continue
			}

			for _, msg := range messages {
				msgId := msg.Id.Hex()

				err := repo.MarkProcessing(ctx, msgId)
				if err != nil {
					log.Printf("failed to mark procedding %v", err)
					continue
				}

				rabbitMsg := mq.Message{
					MessageId: msgId,
					Payload:   msg.Payload,
				}

				err = rabbit.PublishSync(ctx, rabbitMsg)
				if err != nil {
					log.Printf("failed to publish msg %v", err)
					_ = repo.MarkFailed(ctx, msgId, err.Error())
					continue
				}

				err = repo.MarkSent(ctx, msgId)
				if err != nil {
					continue
				}

				log.Printf("publish msg successfully")
			}
			continue
		}
	}
}
