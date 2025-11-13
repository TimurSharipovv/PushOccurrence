package db

import (
	"context"
	"log"
)

type Message struct {
	ID        string
	TableName string
	Operation string
	Payload   string
}

func (d *Db) GetUnprocessedByID(ctx context.Context, id string) (*Message, error) {
	row := d.Conn.QueryRow(ctx, `
		SELECT id::text, table_name, operation, payload::text
		FROM data_exchange.message_queue_log
		WHERE id = $1 AND processed = false
		FOR UPDATE SKIP LOCKED
	`, id)

	var m Message
	if err := row.Scan(&m.ID, &m.TableName, &m.Operation, &m.Payload); err != nil {
		return nil, err
	}
	return &m, nil
}

func (d *Db) MarkProcessed(ctx context.Context, id string) {
	_, err := d.Conn.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET processed = true, updated_at = now()
		WHERE id = $1
	`, id)
	if err != nil {
		log.Printf("failed to mark processed: %v", err)
	}
}
