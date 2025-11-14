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
