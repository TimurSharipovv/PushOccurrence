package pg

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ListenNotifications(ctx context.Context, conn *pgxpool.Conn, notifyCh chan<- *pgconn.Notification) {
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
