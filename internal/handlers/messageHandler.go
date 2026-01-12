package handlers

import (
	"context"
	"log"

	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleMessage(ctx context.Context, pool *pgxpool.Pool, rabbit *mq.Mq, messageID string) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Printf("failed to acquire db connection: %v", err)
		return
	}
	defer conn.Release()

	row := conn.QueryRow(ctx, `
		SELECT message_type FROM data_exchange.message_queue_log
		WHERE message_id=$1 AND transferred=false
		FOR UPDATE SKIP LOCKED
	`, messageID)

	var messageType string
	if err := row.Scan(&messageType); err != nil {
		log.Printf("failed to scan message_type: %v", err)
		return
	}

	row2 := conn.QueryRow(ctx, `
		SELECT message_body FROM data_exchange.message_queue_log_data
		WHERE message_id=$1
	`, messageID)

	var body []byte
	if err := row2.Scan(&body); err != nil {
		log.Printf("failed to scan message_body: %v", err)
		return
	}

	if err := rabbit.Publish(body); err != nil {
		log.Printf("failed to publish to rabbit: %v", err)
		return
	}

	_, err = conn.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET transferred=true, transfer_time=now()
		WHERE message_id=$1
	`, messageID)
	if err != nil {
		log.Printf("failed to update transferred: %v", err)
	}
}
