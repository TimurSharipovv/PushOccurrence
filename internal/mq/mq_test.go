package mq_test

/*
import (
	"context"
	"testing"
	"time"

	"PushOccurrence/internal/mq"
)

func TestPublishSucces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m := mq.NewMq(ctx, "amqp://guest:guest@localhost:5672/", "test_queue")
	if m == nil {
		t.Fatal("mq is nil, rabbit not available")
	}
	defer m.Close()

	err := m.Publish([]byte(`{"test":"ok"}`))
	if err != nil {
		t.Fatalf("expected publish success, got error: %v", err)
	}
}

func TestNewMq_RabbitUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	producer := mq.NewMq(
		ctx,
		"amqp://guest:guest@localhost:5673/",
		"test_queue",
	)
	duration := time.Since(start)

	if producer != nil {
		t.Fatal("expected nil producer when RabbitMQ is unavailable")
	}

	if duration < 3*time.Second {
		t.Fatalf("NewMq returned too early, waited only %v", duration)
	}
}

func TestPublish_WhenConnectionLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer := mq.NewMq(
		ctx,
		"amqp://guest:guest@localhost:5672/",
		"test_queue",
	)
	if producer == nil {
		t.Skip("RabbitMQ not running")
	}
	defer producer.Close()

	//Симулируем падение RabbitMQ
	producer.Conn.Close()

	err := producer.Publish([]byte(`{"event":"test"}`))
	if err == nil {
		t.Fatal("expected error when publishing with closed connection")
	}

	select {
	case <-producer.RetryBuffer():
	default:
		t.Fatal("expected message to be pushed into retry buffer")
	}
}

func TestMonitor_DetectsConnectionLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mq := mq.NewMq(ctx, "amqp://guest:guest@localhost:5672/", "test_queue")
	if mq == nil {
		t.Fatal("mq is nill")
	}

	if !mq.IsConnected() {
		t.Fatalf("expected to be connected initially")
	}

	err := mq.Channel.Close()
	if err != nil {
		t.Fatalf("failed to close chan: %v", err)
	}

	time.Sleep(2 * time.Second)

	if mq.IsConnected() {
		t.Fatal("expected monitor to detect lost conn")
	}
}

func TestReconnect_RestoresConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mq := mq.NewMq(ctx, "amqp://guest:guest@localhost:5672/", "test_queue_reconnect")
	if mq == nil {
		t.Fatal("mq is nil")
	}

	if !mq.IsConnected() {
		t.Fatal("expected initial connection")
	}

	// Рвём соединение
	err := mq.Channel.Close()
	if err != nil {
		t.Fatalf("failed to close channel: %v", err)
	}

	// Ждём, пока monitor зафиксирует разрыв
	time.Sleep(2 * time.Second)

	if mq.IsConnected() {
		t.Fatal("expected connection to be marked as lost")
	}

	// Ждём reconnect
	success := false
	for i := 0; i < 10; i++ {
		if mq.IsConnected() {
			success = true
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !success {
		t.Fatal("expected reconnectLoop to restore connection")
	}
}
*/
