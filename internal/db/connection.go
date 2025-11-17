package db

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

var Conn *pgx.Conn
var WorkerConn *pgx.Conn
var workerErr error

func Init(ctx context.Context, connectionString string) {
	var listenErr error
	Conn, listenErr = pgx.Connect(ctx, connectionString)
	if listenErr != nil {
		log.Println(listenErr)
	}
}

// func InitWorker(ctx context.Context, connectionString string) {
// 	WorkerConn, workerErr = pgx.Connect(ctx, connectionString)
// 	if workerErr != nil {
// 		log.Println(workerErr)
// 	}
// }
