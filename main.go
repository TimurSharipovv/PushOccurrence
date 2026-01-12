package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"PushOccurrence/config"
	"PushOccurrence/internal/db"
	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load("config/config.json")
	if err != nil {
		log.Fatalf("can't load config: %v", err)
	}

	pgConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
		cfg.Postgres.SSLMode,
	)

	db.Init(ctx, pgConnStr)
	defer db.Conn.Close(ctx)

	for _, channel := range cfg.Listener.Channels {
		_, err := db.Conn.Exec(ctx, "LISTEN "+channel)
		if err != nil {
			log.Fatalf("failed to listen on %s: %v", channel, err)
		}
		log.Printf("listening on channel: %s", channel)
	}

	rabbit := mq.NewMq(
		cfg.RabbitMQ.URL,
		cfg.RabbitMQ.Queue.Name,
	)
	defer rabbit.Close()

	log.Println("service started, waiting for notifications...")

	for {
		notification, err := db.Conn.WaitForNotification(ctx)
		if err != nil {
			log.Printf("error waiting for notify: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		payload := notification.Payload
		channel := notification.Channel

		log.Printf(
			"received notify: channel=%s payload=%s",
			channel,
			payload,
		)

		handlers.HandleMessage(ctx, db.Conn, rabbit, payload)
	}
}
