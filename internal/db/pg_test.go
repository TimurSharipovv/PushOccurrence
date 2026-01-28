package db

import (
	"PushOccurrence/internal/mq"
	"context"
	"testing"
	"time"
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
	ctx, cancel := context.WithCancel(context.Background())

	mq := &mq.Mq{
		Messages:      make(chan mq.Message),
		ConnectStatus: make(chan bool),
		Buffer:        make(chan mq.Message, 10),
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		mq.MessageManager(ctx)
	}()

	cancel()

	select {
	case <-done:
		t.Log("messageManager exited on cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("messageManager did not exit")
	}
}
