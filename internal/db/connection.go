package db

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

var Conn *pgx.Conn

func Init(ctx context.Context, connectionString string) {
	var err error
	Conn, err = pgx.Connect(ctx, connectionString)
	if err != nil {
		log.Println(err)
	}
}

// func Connect(){
// 	ctx := context.Background()

// 	conn, err := pgx.Connect(ctx, "postgres://postgres:postgres@localhost:5432/message_queue_db")
// 	if err != nil {
// 		log.Fatalf("failed to connect to postgres: %v", err)
// 	}
// 	defer conn.Close(ctx)

// 	_, err = conn.Exec(ctx, "LISTEN queue_message_log")
// 	if err != nil {
// 		log.Fatalf("failed to listen: %v", err)
// 	}
// 	fmt.Println("Listening for NOTIFY events...")

// 	for {
// 		notification, err := conn.WaitForNotification(ctx)
// 		if err != nil {
// 			log.Printf("error waiting for notify: %v", err)
// 			time.Sleep(2 * time.Second)
// 			continue
// 		}

// 		id := notification.Payload
// 		fmt.Printf("New message event: %s\n", id)

// 		handlers.HandleMessage(ctx, conn, ch, queueName, id)
// 	}
// }

// import (
// 	"context"
// 	"fmt"
// 	"os"

// 	"github.com/jackc/pgx/v5"
// 	"github.com/jackc/pgx/v5/pgxpool"
// )

// type Db struct {
// 	Conn   *pgx.Conn
// 	worker *pgxpool.Pool
// }

// func New(ctx context.Context) (*Db, error) {
// 	connStr := os.Getenv("db_URL")
// 	if connStr == "" {
// 		connStr = "postgres://postgres:postgres@localhost:5432/message_queue_db"
// 	}

// 	conn, err := pgx.Connect(ctx, connStr)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed connection %w", err)
// 	}

// 	return &Db{Conn: conn}, nil
// }

// func (d *Db) Close(ctx context.Context) error {
// 	return d.Conn.Close(ctx)
// }
