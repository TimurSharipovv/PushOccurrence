package handlers

import (
	"context"
	"fmt"
	"log"

	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5"
)

func HandleMessage(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq, id string) {
	var messageType string

	err := pgConn.QueryRow(ctx, `
		SELECT message_type
		FROM data_exchange.message_queue_log
		WHERE message_id = $1 AND transferred = false
		FOR UPDATE SKIP LOCKED
	`, id).Scan(&messageType)

	if err != nil {
		log.Printf("skip (maybe processed or not found): %v", err)
		return
	}

	var messageBody []byte

	err = pgConn.QueryRow(ctx, `
		SELECT message_body
		FROM data_exchange.message_queue_log_data
		WHERE message_id = $1
	`, id).Scan(&messageBody)

	if err != nil {
		log.Printf("failed to read message body: %v", err)
		return
	}

	fmt.Printf(
		"processing message id=%s, type=%s, body=%s\n",
		id, messageType, string(messageBody),
	)

	err = rabbit.Publish(messageBody)
	if err != nil {
		log.Printf("failed to publish message to RabbitMQ: %v", err)
		return
	}

	_, err = pgConn.Exec(ctx, `
		UPDATE data_exchange.message_queue_log
		SET transferred = true, transfer_time = now()
		WHERE message_id = $1
	`, id)

	if err != nil {
		log.Printf("failed to update transferred status: %v", err)
	}

	// log.Printf("update res %v", res.RowsAffected())
}

// func HandleMessage(ctx context.Context, pgConn *pgx.Conn, rabbit *mq.Mq, id string) error {
// 	// 1. Начинаем транзакцию
// 	tx, err := pgConn.Begin(ctx)
// 	if err != nil {
// 	return fmt.Errorf("failed to begin transaction: %w", err)
// 	}
// 	// Гарантируем откат, если не будет Commit
// 	defer tx.Rollback(ctx)

// 	var messageType string

// 	// 2. Блокируем строку внутри транзакции. Блокировка висит до вызова Commit()
// 	err = tx.QueryRow(ctx, `
// 	SELECT message_type
// 	FROM data_exchange.message_queue_log
// 	WHERE message_id = $1 AND transferred = false
// 	FOR UPDATE SKIP LOCKED`, id).Scan(&messageType)

// 	if err != nil {
// 	if err == pgx.ErrNoRows {
// 	// Если строки нет или она заблокирована другим воркером — это не ошибка, просто пропускаем
// 	return nil
// 	}
// 	return fmt.Errorf("select lock error: %w", err)
// 	}

// 	var messageBody []byte

// 	// Читаем тело (тоже внутри транзакции для консистентности)
// 	err = tx.QueryRow(ctx,`SELECT message_body
// 	FROM data_exchange.message_queue_log_data
// 	WHERE message_id = $1`, id).Scan(&messageBody)

// 	if err != nil {
// 	return fmt.Errorf("failed to read body: %w", err)
// 	}

// 	fmt.Printf("processing message id=%s, type=%s\n", id, messageType)

// 	// 3. Отправляем в RabbitMQ
// 	err = rabbit.Publish(messageBody)
// 	if err != nil {
// 	// ВАЖНО: Если рэббит упал, мы возвращаем ошибку.
// 	// Транзакция откатится (defer Rollback), статус в БД останется false.
// 	// Вызывающий код (ProcessBacklog) должен увидеть ошибку и сделать паузу.
// 	log.Printf("failed to publish to RabbitMQ: %v", err)
// 	return err
// 	}

// 	// 4. Обновляем статус
// 	_, err = tx.Exec(ctx, `
// 	UPDATE data_exchange.message_queue_log
// 	SET transferred = true, transfer_time = now()
// 	WHERE message_id = $1, id)

// 	if err != nil {
// 	return fmt.Errorf("failed to update status: %w", err)
// 	}

// 	// 5. Фиксируем транзакцию. Только здесь блокировка снимется и статус обновится.
// 	if err := tx.Commit(ctx); err != nil {
// 	return fmt.Errorf("failed to commit transaction: %w", err)
// 	}

//     return nil
// }
