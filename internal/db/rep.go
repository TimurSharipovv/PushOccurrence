package db

// import (
// 	"context"
// )

// type Message struct {
// 	ID        string
// 	TableName string
// 	Operation string
// 	Payload   string
// }

// func (db *Db) GetUnprocessedByID(ctx context.Context, id string) (*Message, error) {
// 	row := db.worker.QueryRow(ctx,
// 		`SELECT id, table_name, operation, payload
//          FROM data_exchange.message_queue_log
//          WHERE id = $1 AND processed = false`,
// 		id,
// 	)
// 	var m Message
// 	err := row.Scan(&m.ID, &m.TableName, &m.Operation, &m.Payload)
// 	return &m, err
// }

// func (db *Db) MarkProcessed(ctx context.Context, id string) error {
// 	_, err := db.worker.Exec(ctx,
// 		`UPDATE data_exchange.message_queue_log
//          SET processed = true WHERE id = $1`,
// 		id,
// 	)
// 	return err
// }
