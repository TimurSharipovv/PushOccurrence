package mongoDb

import (
	"context"
	"testing"
	"time"
)

// 1 Тест. Входные данные корректны. Пытаемся подключиться к запущенной mongoDB. PASS
func TestConnectSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "mongodb://localhost:27017/outbox"

	t.Logf("try connect to mongo %s", url)

	client, err := Connect(ctx, url)

	t.Logf("Connect: client %p (is nil? %v), err %v", client, client == nil, err)

	if err != nil {
		t.Logf("cant connect to mongo")
		return
	}

	if client == nil {
		t.Fatalf("conn not successfully")
	}

	err = client.Disconnect(ctx)
	if err != nil {
		t.Errorf("error close conn %v", err)
	}
}

// 2 Тест. Проверяем функцию Insert тестовыми данными PASS.
func TestInsertSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "mongodb://localhost:27017/outbox"
	client, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Errorf("disconnect error: %v", err)
		}
	}()

	db := client.Database("outbox")
	repo := NewOutboxRepository(db)

	msg := OutboxMessage{
		Topic:   "test_topic",
		Payload: []byte(`{"key": "value"}`),
	}

	err = repo.Insert(ctx, msg)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	} else {
		t.Logf("Insert success")
	}
}

// 3 Тест. Кладем сообщение в бд и читаем все существующие. Проверяем функцию FetchPending PASS.
func TestFetchPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "mongodb://localhost:27017/outbox"

	client, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Errorf("disconnect error: %v", err)
		}
	}()

	db := client.Database("outbox")
	repo := NewOutboxRepository(db)

	msg := OutboxMessage{
		Topic:   "fetch_test",
		Payload: []byte(`{"data": "fetch_me"}`),
	}
	if err := repo.Insert(ctx, msg); err != nil {
		t.Fatalf("Setup failed (Insert): %v", err)
	}

	messages, err := repo.FetchPending(ctx, 10)

	if err != nil {
		t.Errorf("FetchPending failed: %v", err)
	}

	if len(messages) == 0 {
		t.Errorf("Expected at least one pending message, but got 0")
	}

	for _, m := range messages {
		if m.Status != "pending" {
			t.Errorf("Fetched message with unexpected status: %s", m.Status)
		}
	}
	t.Logf("Fetched %d messages successfully", len(messages))
}
