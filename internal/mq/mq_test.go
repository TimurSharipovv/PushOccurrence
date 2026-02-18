package mq

import (
	"bytes"
	"context"
	"testing"
	"time"

	mongoDb "PushOccurrence/internal/db/mongo"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 1 Тест. нет подключения на старте - надо подключиться(PASS)
func TestConnectToRabbit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	url := "amqp://guest:guest@localhost:5672/"
	queue := "test_queue"

	mq := InitMq(ctx, url, queue)

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

	mq := InitMq(ctx, url, queue)
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

// 3 Тест. Соединение упало при входящем потоке уведомлений - уведомления должны записмываться в mongo

// 4 Тест. при отсутствии соединения Publish должен пиать в буфер(PASS)

// 5 Тест. проверка очистки буфера при появлении соединения - при удачном подключении буфер проверяется на наличие неотправленных сообщений.
// При наличии сообщения должны отправляться в очередь и после успешной доставки удаляться(удаление еще не реализовано, проверяем только доставку) из буфера
// (PASS)

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

	/* 	mq := &Mq{
		Conn:    conn,
		Channel: ch,
		Queue:   queueName,
		Buffer:  buffer,
	} */

	payload := []byte(`{"event":"success_publish"}`)

	/* msg := Message{
		Payload: payload,
	}

	mq.Publish(msg) */

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

// 8 Тест. Проверяем Fallback в Mongo (SendToOutbox) PASS
func TestSendToOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "mongodb://localhost:27017/outbox"
	client, err := mongoDb.Connect(ctx, url)
	if err != nil {
		t.Fatalf("failed to connect to mongo: %v", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("outbox")
	repo := mongoDb.NewOutboxRepository(db)

	testPayload := []byte(`{"event":"fallback_test"}`)
	msg := Message{
		MessageId: "uuid-1234-5678",
		Payload:   testPayload,
	}

	if err := SendToOutbox(ctx, msg, repo); err != nil {
		t.Fatalf("SendToOutbox failed: %v", err)
	}

	coll := db.Collection("messages")
	var foundMsg mongoDb.OutboxMessage

	filter := bson.M{"topic": "failed_rabbit_msg"}

	opts := options.FindOne().SetSort(bson.M{"createdAt": -1})

	err = coll.FindOne(ctx, filter, opts).Decode(&foundMsg)
	if err != nil {
		t.Fatalf("Failed to find message in Mongo: %v", err)
	}

	if !bytes.Equal(foundMsg.Payload, testPayload) {
		t.Errorf("Payload mismatch. Expected %s, got %s", testPayload, foundMsg.Payload)
	}

	t.Log("SendToOutbox test passed")
}

// Вспомогательные функции
func (mq *Mq) IsConnected() bool {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()
	return mq.Conn != nil
}
