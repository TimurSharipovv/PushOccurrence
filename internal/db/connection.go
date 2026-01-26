package db

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

var Pool *pgxpool.Pool

func Init(ctx context.Context, connectionString string) {
	var err error
	Pool, err = pgxpool.New(ctx, connectionString)
	if err != nil {
		log.Fatalf("failed to create pgx pool: %v", err)
	}

	if err := Pool.Ping(ctx); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}
}

func Close() {
	if Pool != nil {
		Pool.Close()
	}
}
