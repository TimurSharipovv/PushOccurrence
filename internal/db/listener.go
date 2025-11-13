package db

import (
	"context"
	"log"
	"time"
)

func (d *Db) listen(ctx context.Context, channel string, handle func(ctx context.Context, id string)) error {
	_, err := d.Conn.Exec(ctx, "LISTEN "+channel)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Printf("Слушается канал: %s", channel)

	for {
		notification, err := d.Conn.WaitForNotification(ctx)
		if err != nil {
			log.Println(err)
			time.Sleep(2 * time.Second)
			continue
		}

		id := notification.Payload
		go handle(ctx, id)
	}
}
