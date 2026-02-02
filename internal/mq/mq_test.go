package mq

import (
	"bytes"
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
		Messages:      make(chan Message),
		ConnectStatus: make(chan bool, 1),
		Buffer:        make(chan Message, 1),
	}

	log.Println("create new mq successfully")

	log.Println("run goroutine")
	go mq.MessageManager(ctx)
	log.Println("goroutine run successfully")

	mq.ConnectStatus <- false
	log.Println("have no connection to mq")

	mq.Messages <- Message{
		MessageId: "8",
		Payload:   []byte("test_message"),
	}

	time.Sleep(300 * time.Millisecond)

	select {
	case msg := <-mq.Buffer:
		if msg.Payload != nil {
			return
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cant write to buffer")
	}
}

// 4 Тест. при отсутствии соединения Publish должен пиать в буфер(PASS)
func TestPublishConnLost(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Fatalf("failed to connect to mq: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}

	err = ch.Confirm(false)
	if err != nil {
		t.Fatalf("failed to enable confirm mode: %v", err)
	}

	_, err = ch.QueueDeclare(
		"test_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("queue declare error: %v", err)
	}

	buffer := make(chan Message, 1)

	mq := &Mq{
		Conn:    conn,
		Channel: ch,
		Queue:   "test_queue",
		Buffer:  buffer,
	}

	_ = conn.Close()

	time.Sleep(100 * time.Millisecond)

	msg := Message{
		Payload: []byte(`{"event":"connection_lost"}`),
	}

	mq.Publish(msg)

	select {
	case bufferedMsg := <-buffer:
		if string(bufferedMsg.Payload) != string(msg.Payload) {
			t.Fatalf("unexpected buffered message")
		}
	case <-time.After(time.Second):
		t.Fatal("expected message to be written to buffer")
	}
}

// 5 Тест. проверка очистки буфера при появлении соединения - при удачном подключении буфер проверяется на наличие неотправленных сообщений.
// При наличии сообщения должны отправляться в очередь и после успешной доставки удаляться(удаление еще не реализовано, проверяем только доставку) из буфера
// (PASS)
func TestCleaningBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := "amqp://guest:guest@localhost:5672/"
	queue := "testQ"

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm mode error: %v", err)
	}

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("queue declare error: %v", err)
	}

	mq := &Mq{
		Buffer:          make(chan Message, 10),
		Messages:        make(chan Message, 10),
		ConnectStatus:   make(chan bool, 1),
		RePublishStatus: make(chan bool, 1),
		Channel:         ch,
		Queue:           queue,
	}

	go mq.MessageManager(ctx)

	mq.Messages <- Message{
		MessageId: "8",
		Payload:   []byte("test_message"),
	}

	msg := <-mq.Messages

	mq.sendToBuffer(msg)

	mq.ConnectStatus <- true
	mq.RePublishStatus <- true

	time.Sleep(200 * time.Millisecond)

	deliveredMsg, ok, err := ch.Get(queue, true)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !ok {
		t.Fatal("message not delivered")
	}

	if !bytes.Equal(deliveredMsg.Body, msg.Payload) {
		t.Fatalf("message mismatch")
	}

	t.Logf("message successfully delivered: %s, %s", deliveredMsg.Body, msg.Payload)
}

// 6 Тест. проверка работоспособности функции monitor - каждые 5 секунд на протяжении 30 секунд подключаем и отключаем брокер.
// Наша функция должна успешно менять значение в канале Connect при каждом изменении
func TestMonitor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq := &Mq{
		ConnectStatus: make(chan bool, 1),
	}

	go mq.Monitor(ctx)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		connected := false

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if connected {
					mq.Channel = nil
					mq.Conn = nil
					t.Log("conn lost")
				} else {
					mq.Channel = &amqp.Channel{}
					mq.Conn = &amqp.Connection{}
					t.Log("conn up")
				}
				connected = !connected
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			t.Log("test finished")
			return
		case status := <-mq.ConnectStatus:
			if status {
				t.Log("true")
			} else {
				t.Log("false")
			}
		}
	}
}

// 7 Тест. все хорошо - сообщение должно успешно доставляться в очередь. PASS
func TestPublishMessageDelivered(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Fatalf("failed to connect to mq: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.Confirm(false)
	if err != nil {
		t.Fatalf("failed to enable confirm mode: %v", err)
	}

	queueName := "test_publish_queue"

	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("queue declare error: %v", err)
	}

	buffer := make(chan Message, 1)

	mq := &Mq{
		Conn:    conn,
		Channel: ch,
		Queue:   queueName,
		Buffer:  buffer,
	}

	payload := []byte(`{"event":"success_publish"}`)

	msg := Message{
		Payload: payload,
	}

	mq.Publish(msg)

	select {
	case <-buffer:
		t.Fatal("message should not be written to buffer on successful publish")
	case <-time.After(300 * time.Millisecond):
	}

	deliveries, err := ch.Consume(
		q.Name,
		"",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	select {
	case d := <-deliveries:
		if string(d.Body) != string(payload) {
			t.Fatalf("unexpected message body: %s", d.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("message was not delivered to queue")
	}
}

// Вспомогательные функции
func (mq *Mq) IsConnected() bool {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()
	return mq.Conn != nil
}
