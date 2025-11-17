package db

import (
	"context"
	"log"

	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
)

func processBacklog(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq) {
	for {
		rows, err := pgConn.Query(ctx, `
		select message_id::text from data_exchange.message_queue_log
		where transferred = false
		order by message_time limit 100`)

		if err != nil {
			log.Printf("query error: %w", err)
			return
		}

		ids := make([]string, 0)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				log.Printf("scan error: %v", err)
				continue
			}
			ids = append(ids, id)
		}
		rows.Close()

		if len(ids) == 0 {
			log.Printf("no pending message: ")
			return
		}

		log.Printf("found %d pending message", len(ids))

		for _, id := range ids {
			handlers.HandleMessage(ctx, pgConn, rabbit, id)
		}
	}
}
