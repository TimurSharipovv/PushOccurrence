package handlers

import (
	"context"
	"fmt"
	"log"

	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
)

func HandleMessage(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq, id string) {
	var messageType string

	err := pgConn.QueryRow(ctx, `
		SELECT message_type
		FROM data_exchange.message_queue_log
		WHERE message_id = $1 AND transferred = false
		FOR UPDATE SKIP LOCKED
	`, id).Scan(&messageType)

	if err != nil {
		log.Printf("skip: %v", err)
		return
	}

	var messageBody []byte

	err = pgConn.QueryRow(ctx, `
    SELECT message_body
    FROM data_exchange.message_queue_log_data
    WHERE message_id = $1
	`, id).Scan(&messageBody)

	if err != nil {
		log.Printf("bad message: no body for id=%s (%v)", id, err)

		_, updErr := pgConn.Exec(ctx, `
			UPDATE data_exchange.message_queue_log
			SET transferred = true, transfer_time = now()
			WHERE message_id = $1
		`, id)

		if updErr != nil {
			log.Printf("failed to mark bad message as transferred: %v", updErr)
		}

		return
	}

	fmt.Printf(
		"processing message id=%s, type=%s, body=%s\n",
		id, messageType, string(messageBody),
	)

	err = rabbit.Publish(messageBody)
	if err != nil {
		log.Printf("failed to publish message to RabbitMQ: %v", err)
		return
	}

	// _, err = pgConn.Exec(ctx, `
	// 	UPDATE data_exchange.message_queue_log
	// 	SET transferred = true, transfer_time = now()
	// 	WHERE message_id = $1
	// `, id)

	// if err != nil {
	// 	log.Printf("failed to update transferred status: %v", err)
	// }

	// log.Printf("update res %v", res.RowsAffected())
}
