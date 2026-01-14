package service

import (
	"PushOccurrence/internal/db"
	"context"
	"log"
	"os"
	"time"

	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListenNotifications(ctx context.Context, conn *pgxpool.Conn, notifyCh chan<- *pgconn.Notification) {
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
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
}

func AcquireConn(ctx context.Context) *pgxpool.Conn {
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("failed to acquire connection: %v", err)
	}
	return conn
}

func ListenChannels(ctx context.Context, conn *pgxpool.Conn, channels []string) {
	for _, ch := range channels {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			log.Fatalf("failed to listen on %s: %v", ch, err)
		}
		log.Printf("listening on channel: %s", ch)
	}
}

func MainLoop(ctx context.Context, notifyCh <-chan *pgconn.Notification, sigCh <-chan os.Signal, rabbit *mq.Mq) {
	for {
		select {
		case notification := <-notifyCh:
			go handlers.HandleMessage(ctx, db.Pool, rabbit, notification.Payload)
			log.Printf("received notify: channel=%s payload=%s", notification.Channel, notification.Payload)

		case sig := <-sigCh:
			log.Printf("received signal %s, shutting down...", sig)
			return

		case <-ctx.Done():
			return
		}
	}
}
