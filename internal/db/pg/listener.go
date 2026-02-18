package pg

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RunListener(ctx context.Context, pool *pgxpool.Pool, channels []string, notifyCh chan<- *pgconn.Notification) {
	backoff := time.Second

	for {
		err := startListening(ctx, pool, channels, notifyCh)

		if errors.Is(err, context.Canceled) {
			log.Printf("ctx canceled, stopping")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func startListening(ctx context.Context, pool *pgxpool.Pool, channels []string, notifyCh chan<- *pgconn.Notification) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	for _, ch := range channels {
		_, err := conn.Exec(ctx, "LISTEN "+ch)
		if err != nil {
			return err
		}

		log.Printf("listener sub successfully %s", ch)
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case notifyCh <- notification:

		}
	}
}
