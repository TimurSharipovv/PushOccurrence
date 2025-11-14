package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"PushOccurrence/internal/handlers"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5432/message_queue_db")
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "LISTEN queue_message_log")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Println("Listening for NOTIFY events...")

	rabbitConn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	queueName := "message_queue"
	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			log.Printf("error waiting for notify: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		id := notification.Payload
		fmt.Printf("New message event: %s\n", id)

		handlers.HandleMessage(ctx, conn, ch, queueName, id)
	}
}

func handleMessage(ctx context.Context, pgConn *pgx.Conn, mqCh *amqp.Channel, queueName, id string) {
	var payload, operation, tableName string
	err := pgConn.QueryRow(ctx, `
		SELECT payload::text, operation, table_name
		FROM data_exchange.message_queue_log
		WHERE id = $1 AND processed = false
		FOR UPDATE SKIP LOCKED
	`, id).Scan(&payload, &operation, &tableName)

	if err != nil {
		log.Printf("skip (maybe already processed): %v", err)
		return
	}

	fmt.Printf("Processing change from table %s (%s): %s\n", tableName, operation, payload)

	err = mqCh.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(payload),
		},
	)
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
