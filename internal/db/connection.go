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
