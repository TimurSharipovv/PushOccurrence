package db

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListenNotifications(
	ctx context.Context,
	conn *pgxpool.Conn,
	notifyCh chan<- *pgconn.Notification,
) {
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				log.Println("ListenNotifications stopped: context canceled")
				return
			}

			if isConnectionError(err) {
				log.Printf("ListenNotifications fatal error: %v", err)
				return
			}

			log.Printf("ListenNotifications temporary error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case notifyCh <- notification:
		}
	}
}

func AcquireConn(ctx context.Context) *pgxpool.Conn {
	conn, err := Pool.Acquire(ctx)
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

func MainLoop(ctx context.Context, notifyCh <-chan *pgconn.Notification, sigCh <-chan os.Signal, rabbit *mq.Mq, cancel context.CancelFunc) {
	for {
		select {
		case notification := <-notifyCh:
			go handlers.HandleMessage(ctx, Pool, rabbit, notification.Payload)
			log.Printf("received notify: channel=%s payload=%s", notification.Channel, notification.Payload)

		case sig := <-sigCh:
			log.Printf("received signal %s, shutting down...", sig)
			cancel()
			return

		case <-ctx.Done():
			return
		}
	}
}

func isConnectionError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "08003", "08006":
			return true
		}
	}

	msg := err.Error()
	return strings.Contains(msg, "conn closed") || strings.Contains(msg, "EOF") || strings.Contains(msg, "broken pipe")
}
