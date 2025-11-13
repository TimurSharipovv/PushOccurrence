package db

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

type Db struct {
	Conn *pgx.Conn
}

func New(ctx context.Context) (*Db, error) {
	connStr := os.Getenv("db_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/message_queue_db"
	}

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed connection %w", err)
	}

	return &Db{Conn: conn}, nil
}

func (d *Db) Close(ctx context.Context) error {
	return d.Conn.Close(ctx)
}
