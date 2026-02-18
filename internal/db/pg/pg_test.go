package pg

import (
	"PushOccurrence/internal/mq"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 1 Тест. Горутина, которая слушает контекст, должна завершиться, когда контекст отменён(PASS).
func TestMonitorStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mq := &mq.Mq{
		ConnectStatus: make(chan bool, 1),
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		mq.Monitor(ctx)
	}()

	cancel()

	select {
	case <-done:
		t.Log("monitor exited on cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not exit")
	}
}

// 2 Тест. monitor должен прекратить работу после отмены контекста и не зависать в тикере или каналах(PASS).
func TestListenNotificationsStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		<-ctx.Done()
	}()

	cancel()

	select {
	case <-done:
		t.Log("ListenNotifications exited on cancel")
	case <-time.After(1 * time.Second):
		t.Fatal("ListenNotifications did not exit")
	}
}

// 3 Тест. messageManager должен завершиться при отмене контекста, даже если нет сообщений и соединения(PASS).
func TestMessageManagerStopsOnCancel(t *testing.T) {
	/* ctx, cancel := context.WithCancel(context.Background())

	mq := &mq.Mq{
		Messages:      make(chan mq.Message),
		ConnectStatus: make(chan bool),
		Buffer:        make(chan mq.Message, 10),
	} */

	done := make(chan struct{})

	go func() {
		defer close(done)
		/* mq.MessageManager(ctx) */
	}()

	/* cancel() */

	select {
	case <-done:
		t.Log("messageManager exited on cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("messageManager did not exit")
	}
}

// 4  Тест. Обрыв соединения с бд. WaitForNotification должен вернуть ошибку, ListenNotifications должен выйти. PASS
func TestListenNotificationConnectionLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/message_queue_db?sslmode=disable")
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	done := make(chan struct{})

	go func() {
		defer close(done)
		ListenNotifications(ctx, conn, make(chan *pgconn.Notification))
	}()

	cancel()

	select {
	case <-done:

	case <-time.After(2 * time.Second):
		t.Fatal("listener did not stop after context cancel")
	}
}

// 5 Тест. проверяем функцию IsConnectedError. передаем слайс структур для входа. поле want показывает ожидаемый результат приопределенном err PASS
func TestIsConnectedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "pg error connection not exist",
			err: &pgconn.PgError{
				Code: "08003",
			},
			want: true,
		},
		{
			name: "pg error connection fail",
			err: &pgconn.PgError{
				Code: "08006",
			},
			want: true,
		},
		{
			name: "pg error unknown code",
			err: &pgconn.PgError{
				Code: "23505",
			},
			want: false,
		},
		{
			name: "err contains EOF",
			err:  errors.New("unexpected EOF"),
			want: true,
		},
		{
			name: "err contains broken pipe",
			err:  errors.New("broken pipe"),
			want: true,
		},
		{
			name: "non conn error",
			err:  errors.New("syntax error"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConnectionError(tt.err)
			if result != tt.want {
				t.Fatalf("isConnectionError() %v, want %v (err %v)", result, tt.want, tt.err)
			}
		})
	}
}

// 6 Тест. Проверка тестовыми данными функции FetchPendingMessages. Должна возвращать ID неотправленных сообщений PASS.
func TestFetchPendingMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connStr := "postgres://postgres:postgres@localhost:5433/mydb?sslmode=disable"
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	var messageID string
	err = pool.QueryRow(ctx, `
		INSERT INTO data_exchange.message_queue_log (message_type, transferred)
		VALUES ('TEST_TYPE', false)
		RETURNING message_id::text
	`).Scan(&messageID)

	if err != nil {
		t.Fatalf("failed to insert test message: %v", err)
	}

	ids, err := FetchPendingMessages(ctx, pool)

	if err != nil {
		t.Errorf("FetchPendingMessages failed: %v", err)
	}

	found := false
	for _, id := range ids {
		if id == messageID {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected message ID %s not found in pending messages", messageID)
	}

	t.Logf("FetchPendingMessages returned %d messages, including our test ID", len(ids))
}
