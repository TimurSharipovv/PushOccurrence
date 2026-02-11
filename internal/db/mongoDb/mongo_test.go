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

// 2 Тест. Проверяем функцию Insert тестовыми данными.
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
