package handlers

import (
	"context"
	"log"

	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleMessage(ctx context.Context, pool *pgxpool.Pool, rabbit *mq.Mq, messageID string) {
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
		WHERE message_id = 
		AND transferred = false
		FOR UPDATE SKIP LOCKED`,
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
		WHERE message_id = 
	`,
		messageID).Scan(&body)

	if err != nil {
		log.Printf("failed to scan message_body for %s: %v", messageID, err)
		return
	}
	err = rabbit.PublishSync(ctx, mq.Message{
		MessageId: messageID,
		Payload:   body,
	})

	if err != nil {
		log.Printf("failed to publish message %s to rabbit: %v", messageID, err)
		return
	}

	_, err = tx.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET transferred = true,
    	transfer_time = now()
		WHERE message_id = `,
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
