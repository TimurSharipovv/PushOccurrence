package db

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

var Conn *pgx.Conn
var WorkerConn *pgx.Conn

func Init(ctx context.Context, connectionString string) {
	var listenErr error
	Conn, listenErr = pgx.Connect(ctx, connectionString)
	if listenErr != nil {
		log.Println(listenErr)
	}
}

/* func BuildConnStr (){
	pgConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
        cfg.Postgres.User,
        cfg.Postgres.Password,
        cfg.Postgres.Host,
        cfg.Postgres.Port,
        cfg.Postgres.Database,
        cfg.Postgres.SSLMode,
	)
} */

// func InitWorker(ctx context.Context, connectionString string) {
// 	WorkerConn, workerErr = pgx.Connect(ctx, connectionString)
// 	if workerErr != nil {
// 		log.Println(workerErr)
// 	}
// }
