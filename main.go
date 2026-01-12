package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"PushOccurrence/config"
	"PushOccurrence/internal/db"
	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

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
	defer db.Close(ctx)

	listenConn, err := db.Pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("failed to acquire connection for listen: %v", err)
	}
	defer listenConn.Release()

	for _, channel := range cfg.Listener.Channels {
		_, err := listenConn.Exec(ctx, "LISTEN "+channel)
		if err != nil {
			log.Fatalf("failed to listen on %s: %v", channel, err)
		}
		log.Printf("listening on channel: %s", channel)
	}

	rabbit := mq.NewMq(cfg.RabbitMQ.URL, cfg.RabbitMQ.Queue.Name)
	defer rabbit.Close()

	log.Println("service started, waiting for notifications...")

	notifyCh := make(chan *pgconn.Notification)

	go func() {
		for {
			notification, err := listenConn.Conn().WaitForNotification(ctx)
			if err != nil {
				log.Printf("error waiting for notify: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			select {
			case notifyCh <- notification:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case notification := <-notifyCh:
			payload := notification.Payload
			channel := notification.Channel

			log.Printf("received notify: channel=%s payload=%s", channel, payload)

			go handlers.HandleMessage(ctx, db.Pool, rabbit, payload)

		case sig := <-sigCh:
			log.Printf("received signal %s, shutting down...", sig)
			cancel()
			time.Sleep(500 * time.Millisecond)
			return
		case <-ctx.Done():
			return
		}
	}
}
