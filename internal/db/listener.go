package db

import (
	"context"
)

func (db *Db) Listen(ctx context.Context, channel string, onEvent func(ctx context.Context, id string)) error {
	_, err := db.Conn.Exec(ctx, "LISTEN "+channel)
	if err != nil {
		return err
	}

	for {
		notification, err := db.Conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}

		go onEvent(ctx, notification.Payload)
	}
}
