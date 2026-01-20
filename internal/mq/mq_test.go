package mq

import (
	"context"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

// 3 Тест. Соединение упало при входящем потоке уведомлений - уведомления должны записмываться в Buffer(PASS)
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

// 4 Тест. все работает хорошо - сообщения должны отправляться в очередь с подтверждением Ack(PASS)
func TestSendToRabbit(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"
	queue := "test"

	// Подключаемся к RabbitMQ
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.Confirm(false)
	if err != nil {
		t.Fatalf("cant put ch to confirm mod %v", err)
	}
	// Создаем очередь
	_, err = ch.QueueDeclare(
		queue,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)

	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}

	mq := &Mq{
		Conn:    conn,
		Channel: ch,
		Queue:   queue,
	}

	msg := []byte(`{"test": "deferred confirm"}`)

	mq.sendToRabbit(msg)

	deliveredMsg, ok, err := ch.Get(queue, true)
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}
	if !ok {
		t.Fatal("message not delivered")
	}

	if string(deliveredMsg.Body) != string(msg) {
		t.Fatalf("message mismatch: got %s, want %s", deliveredMsg.Body, msg)
	}

	t.Logf("message successfully delivered: %s", deliveredMsg.Body)
}

// 5 Тест. проверка очистки буфера при появлении соединения - при удачном подключении буфер проверяется на наличие неотправленных сообщений.
// При наличии сообщения должны отправляться в очередь и после успешной доставки удаляться(удаление еще не реализовано, проверяем только доставку) из буфера
// (FAIL, не работает публикация в очередь, хотя метод sendToRabbit успешно публикует сообщения в случае прихода сообщений из бд)
func TestCleaningBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "amqp://guest:guest@localhost:5672/"
	queue := "test"

	mq := &Mq{
		Buffer:   make(chan []byte, 10),
		Messages: make(chan []byte, 10),
		Connect:  make(chan bool, 1),
	}

	go mq.messageManager(ctx)

	msg := []byte("test message")
	mq.sendToBuffer(msg)
	t.Log("message write successfully")

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	} else {
		log.Println("connect to mq succesfully")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.Confirm(false)
	if err != nil {
		t.Fatalf("cant put ch to confirm mod %v", err)
	}

	_, err = ch.QueueDeclare(
		queue,
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		t.Fatalf("failed to declare %v", err)
	}

	deliveredMsg, ok, err := ch.Get(queue, true)
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}
	if !ok {
		t.Fatal("message not delivered")
	}

	if string(deliveredMsg.Body) != string(msg) {
		t.Fatalf("message mismatch: got %s, want %s", deliveredMsg.Body, msg)
	}

	t.Logf("message successfully delivered: %s", deliveredMsg.Body)

}

// Вспомогательные функции
func (mq *Mq) IsConnected() bool {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()
	return mq.Conn != nil
}
