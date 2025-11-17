package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"PushOccurrence/internal/db"
	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"
)

func main() {
	ctx := context.Background()

	db.Init(ctx, "postgres://postgres:postgres@localhost:5432/message_queue_db")
	defer db.Conn.Close(ctx)

	fmt.Println("listening for NOTIFY events...")

	rabbit := mq.NewMq("amqp://guest:guest@localhost:5672/", "message_queue")
	defer rabbit.Close()

	db.ProcessBacklog(ctx, db.Conn, rabbit)

	// чтобы полностью не зависеть от notify добавляем легкую проверку (каждые ) на случай потери уведомления со стороны notify
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			log.Println("sometimes check")
			db.ProcessBacklog(ctx, db.Conn, rabbit)
		}
	}()

	_, err := db.Conn.Exec(ctx, "LISTEN queue_message_log")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	for {
		notification, err := db.Conn.WaitForNotification(ctx)
		if err != nil {
			log.Printf("error waiting for notify: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		id := notification.Payload
		fmt.Printf("new message event: %s\n", id)

		handlers.HandleMessage(ctx, db.Conn, rabbit, id)
	}
}
