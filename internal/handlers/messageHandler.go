package handlers

import (
	"context"
	"log"

	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleMessage(ctx context.Context, pool *pgxpool.Pool, rabbit *mq.Mq, messageID string) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("failed to acquire db connection: %v", err)
		return
	}
	defer conn.Release()

	err = conn.QueryRow(ctx, `
		SELECT 1
		FROM data_exchange.message_queue_log
		WHERE message_id = $1
		AND transferred = false
		FOR UPDATE SKIP LOCKED
	`, messageID).Scan(new(int))

	if err != nil {
		if err == pgx.ErrNoRows {
			log.Printf("message %s already processed or does not exist", messageID)
		} else {
			log.Printf("failed to scan message_type: %v", err)
		}
		return
	}

	var body []byte
	err = conn.QueryRow(ctx, `
		SELECT message_body
		FROM data_exchange.message_queue_log_data
		WHERE message_id = $1
	`, messageID).Scan(&body)

	if err != nil {
		log.Printf("failed to scan message_body for %s: %v", messageID, err)
		return
	}

	deliveryTag, err := rabbit.Publish(body)
	if err != nil {
		log.Printf("failed to publish message %s: %v", messageID, err)
		return
	}

	_, err = conn.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET transferred = true,
    	transfer_time = now()
		WHERE message_id = $1
	`, messageID)

	if err != nil {
		log.Printf("failed to update log table for message %s: %v", messageID, err)
		return
	}

	log.Printf(
		"message %s successfully sent to RabbitMQ, deliveryTag=%d",
		messageID,
		deliveryTag,
	)
}
