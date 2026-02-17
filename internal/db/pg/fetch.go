package pg

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func FetchPendingMessages(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	var ids []string
	rows, err := pool.Query(ctx, `
		SELECT message_id::text
		FROM data_exchange.message_queue_log
		WHERE transferred = false
		ORDER BY message_time ASC
		LIMIT 1000
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
