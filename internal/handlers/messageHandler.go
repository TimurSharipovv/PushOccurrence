package handlers

import (
	"context"
	"fmt"
	"log"

	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
)

func HandleMessage(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq, id string) {
	var payload, operation, tableName string
	err := pgConn.QueryRow(ctx, `
		SELECT payload::text, operation, table_name
		FROM data_exchange.message_queue_log
		WHERE id = $1 AND processed = false
		FOR UPDATE SKIP LOCKED
	`, id).Scan(&payload, &operation, &tableName)

	if err != nil {
		log.Printf("skip: %v", err)
		return
	}

	fmt.Printf("process changing %s (%s): %s\n", tableName, operation, payload)

	err = rabbit.Publish([]byte(payload))
	if err != nil {
		log.Printf("failed to publish to RabbitMQ: %v", err)
		return
	}

	_, err = pgConn.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET processed = true, updated_at = now()
		WHERE id = $1
	`, id)
	if err != nil {
		log.Printf("failed to mark processed: %v", err)
	}
}
