package db

// import (
// 	"context"
// 	"time"

// 	"PushOccurrence/internal/handlers"
// 	"PushOccurrence/internal/mq"

// 	"github.com/jackc/pgx/v5"
// )

// // функция находит список неотправленных сообщений по id
// func FetchBacklogIds(ctx context.Context, conn *pgx.Conn, limit int) ([]string, error) {
// 	rows, err := conn.Query(ctx, `
//         SELECT message_id::text
//         FROM data_exchange.message_queue_log
//         WHERE transferred = false
//         ORDER BY message_time
//         LIMIT $1
//     `, limit)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var ids []string
// 	for rows.Next() {
// 		var id string
// 		if err := rows.Scan(&id); err != nil {
// 			return nil, err
// 		}
// 		ids = append(ids, id)
// 	}
// 	return ids, rows.Err()
// }

// // функция запускается отдельной горутиной для поиска неотпрпавленных сообщений с помощью FetchBacklogIds с интервалом в 10 сек
// // обработка сообщений через HandleMessage
// // вызов при старте после подключения к pq и mq, но строго до начала WaitFotNotification
// func RunBacklogWorker(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq, notifyChan <-chan string) {

// 	for {
// 		select {
// 		case id := <-notifyChan:
// 			handlers.HandleMessage(ctx, pgConn, rabbit, id)

// 		default:
// 			ids, _ := FetchBacklogIds(ctx, pgConn, 100)
// 			for _, bid := range ids {
// 				handlers.HandleMessage(ctx, pgConn, rabbit, bid)
// 			}

// 			time.Sleep(1 * time.Second)
// 		}
// 	}
// }
