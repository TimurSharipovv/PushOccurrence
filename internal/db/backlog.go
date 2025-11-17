package db

import (
	"context"
	"log"

	"PushOccurrence/internal/handlers"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
)

// функция находит все transfered = false и по каждому id вызывает HandleMessage.
// Работает в цикле пока есть необработанные записи.
// Установлен limit 100 чтобы не забирать все строки сразу.
// после снова возвращается в цикл пока не станет пусто
// вызов при старте после подключения к pq и mq, но строго до того как начинаем слушать
func ProcessBacklog(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq) {
	for {
		rows, err := pgConn.Query(ctx, `
		select message_id::text from data_exchange.message_queue_log
		where transferred = false
		order by message_time 
		limit 100`)

		if err != nil {
			log.Printf("query error: %v", err)
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

			if err != nil {
				log.Printf("error processing message %s %v", id, err)
			}
		}
	}
}
