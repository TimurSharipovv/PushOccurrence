package db

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
