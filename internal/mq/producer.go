package mq

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mq struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	Queue       string
	retryBuffer chan []byte
	confirms    <-chan amqp.Confirmation
}

func NewMq(url, queueName string) *Mq {
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error

	retryCount := 5
	for i := 1; i <= retryCount; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("RabbitMQ connection attempt %d/%d failed: %v", i, retryCount, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ after %d attempts: %v", retryCount, err)
	}

	ch, err = conn.Channel()
	if err != nil {
		log.Fatalf("failed to open channel: %v", err)
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		log.Fatalf("failed to put channel in confirm mode: %v", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	mq := &Mq{
		Conn:        conn,
		Channel:     ch,
		Queue:       queueName,
		retryBuffer: make(chan []byte, 100),
		confirms:    confirms,
	}

	go mq.retryLoop()

	return mq
}

func (m *Mq) Publish(payload []byte) (uint64, error) {
	err := m.Channel.Publish(
		"",
		m.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		},
	)
	if err != nil {
		return 0, err
	}

	confirm := <-m.confirms

	if !confirm.Ack {
		return 0, fmt.Errorf("message nack from broker, deliveryTag=%d", confirm.DeliveryTag)
	}

	return confirm.DeliveryTag, nil
}

func (m *Mq) retryLoop() {
	for {
		select {
		case msg := <-m.retryBuffer:
			err := m.Channel.Publish(
				"", m.Queue, false, false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        msg,
				},
			)
			if err != nil {
				log.Println(err)
				go func(msgCopy []byte) {
					time.Sleep(2 * time.Second)
					select {
					case m.retryBuffer <- msgCopy:
					default:
						log.Println(err)
					}
				}(msg)
			} else {
				log.Printf("retry successful")
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (m *Mq) Close() {
	m.Channel.Close()
	m.Conn.Close()
}
