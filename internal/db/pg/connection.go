package pg

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
	log.Println("success create pgx pool")

	err = Pool.Ping(ctx)
	log.Println("try ping to pg")
	if err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}
	log.Println("success ping to pg")
}

func Close() {
	if Pool != nil {
		Pool.Close()
	}
}
