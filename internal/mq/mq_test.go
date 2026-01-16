package mq

import (
	"context"
	"log"
	"testing"
	"time"
)

// 1 Тест. нет подключения на старте - надо подключиться(PASS)
func TestConnectToRabbit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	url := "amqp://guest:guest@localhost:5672/"
	queue := "test_queue"

	mq := CreateMq(ctx, url, queue)

	timeout := time.After(200 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("service didnt connect to mq in time")
		case <-ticker.C:
			if mq.IsConnected() {
				t.Log("mq connect successfully")
				return
			}
		}
	}
}

// 2 Тест. Соединение упало, должен произойти reconnect(PASS)
func TestReconnectAfterBrokerRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	url := "amqp://guest:guest@localhost:5672/"
	queue := "test_queue"

	mq := CreateMq(ctx, url, queue)
	time.Sleep(10 * time.Second)

	t.Log("STOP RabbitMQ now")
	time.Sleep(15 * time.Second)

	if mq.IsConnected() {
		t.Fatal("expected connection to be lost")
	}

	t.Log("START RabbitMQ now")
	time.Sleep(15 * time.Second)

	if !mq.IsConnected() {
		t.Fatal("expected connection to be restored after broker restart")
	}

	t.Log("reconnect successful")
}

// 3 Тест. Соединение упало при входящем потоке уведомлений - уведомления  должны записмываться в Buffer
func TestWriteToBufferAfterConnectionLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("create new mq")
	mq := &Mq{
		Messages: make(chan []byte),
		Connect:  make(chan bool, 1),
		Buffer:   make(chan []byte, 1),
	}

	log.Println("create new mq successfully")

	log.Println("run goroutine")
	go mq.messageManager(ctx)
	log.Println("goroutine run successfully")

	mq.Connect <- false
	log.Println("have no connection to mq")

	mq.Messages <- []byte("test message")

	time.Sleep(300 * time.Millisecond)

	select {
	case payload := <-mq.Buffer:
		if payload != nil {
			return
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cant write to buffer")
	}
}

// Вспомогательные функции для тестов
func (mq *Mq) IsConnected() bool {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()
	return mq.Conn != nil
}
