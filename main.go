package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"PushOccurrence/internal/db"
	"PushOccurrence/internal/mq"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	database, err := db.New(ctx)
	if err != nil {
		log.Fatalf("db connection failed: %v", err)
	}
	defer database.Close(ctx)

	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}
	producer, err := mq.NewProducer(amqpURL, "push_events")
	if err != nil {
		log.Fatalf("mq init failed: %v", err)
	}
	defer producer.Close()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		cancel()
	}()

	if err := database.Listen(ctx, "queue_message_log", func(ctx context.Context, id string) {
		processEvent(ctx, database, producer, id)
	}); err != nil {
		log.Fatalf("listener failed: %v", err)
	}
}

func processEvent(ctx context.Context, database *db.Db, producer *mq.Producer, id string) {
	msg, err := database.GetUnprocessedByID(ctx, id)
	if err != nil {
		log.Printf("skip event %s: %v", id, err)
		return
	}

	body, _ := json.Marshal(map[string]interface{}{
		"id":        msg.ID,
		"table":     msg.TableName,
		"operation": msg.Operation,
		"payload":   json.RawMessage(msg.Payload),
		"ts":        time.Now().UTC(),
	})

	if err := producer.Publish(ctx, body, ""); err != nil {
		log.Printf("failed to publish to mq for id=%s: %v", msg.ID, err)
		return
	}

	database.MarkProcessed(ctx, msg.ID)
	log.Printf("processed id=%s -> published", msg.ID)
}
