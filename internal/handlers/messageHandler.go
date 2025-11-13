package handlers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

func HandleMessage(ctx context.Context, conn *pgx.Conn, id string) {
	var payload, operation, tableName string
	err := conn.QueryRow(ctx, `
	SELECT payload::text, operation, table_name
	FROM data_exchange.message_queue_log
	WHERE id = $1 AND processed = false                  
	FOR UPDATE SKIP LOCKED`,
		id).Scan(&payload, &operation, tableName)

	if err != nil {
		log.Println(err)
		return
	}

	fmt.Printf("change from table %s (%s): %s\n", tableName, operation, payload)

	time.Sleep(1 * time.Second)

	_, err = conn.Exec(ctx, `UPDATE data_exchange.message_queue_log SET proceed = true, updated_at = now() WHERE id = $1`)
	if err != nil {
		log.Println(err)
	}
}
