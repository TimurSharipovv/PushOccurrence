package handlers

import (
	"context"
	"log"
	"time"

	mongoDb "PushOccurrence/internal/db/mongo"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleMessage(ctx context.Context, pool *pgxpool.Pool, rabbit *mq.Mq, mongoRepo mongoDb.OutboxRepositoryInteface, messageID string) {
	var body []byte

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Printf("failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, `
		SELECT 1
		FROM data_exchange.message_queue_log
		WHERE message_id = $1
		AND transferred = false`,
		messageID).Scan(new(int))

	if err != nil {
		if err == pgx.ErrNoRows {
			log.Printf("message %s skipped (locked or sent)", messageID)
		} else {
			log.Printf("failed to scan/lock message: %v", err)
		}
		return
	}

	err = tx.QueryRow(ctx, `
		SELECT message_body
		FROM data_exchange.message_queue_log_data
		WHERE message_id = $1
	`,
		messageID).Scan(&body)

	if err != nil {
		log.Printf("failed to scan message_body for %s: %v", messageID, err)
		return
	}

	msg := mq.Message{
		MessageId: messageID,
		Payload:   body,
	}

	err = rabbit.PublishSync(ctx, msg)
	if err != nil {
		log.Printf("failed to publish message %s to rabbit: %v. Trying fallback to Mongo...", messageID, err)

		errFn := mq.SendToOutbox(ctx, msg, mongoRepo)
		if errFn != nil {
			log.Printf("failed to send to fallback Outbox (Mongo): %v", errFn)
			time.Sleep(5 * time.Second)
			return
		}

		log.Printf("Message %s saved to fallback Outbox (Mongo)", messageID)

		time.Sleep(5 * time.Second)
	}

	_, err = tx.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET transferred = true,
    	transfer_time = now()
		WHERE message_id = $1`,
		messageID)

	if err != nil {
		log.Printf("failed to update log table for message %s: %v", messageID, err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("failed to commit transaction for %s: %v", messageID, err)
		return
	}

	log.Printf("message %s successfully processed and sent", messageID)
}
