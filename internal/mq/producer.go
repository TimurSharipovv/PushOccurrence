package mq

import (
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mq struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	Queue       string
	retryBuffer chan []byte
}

func NewMq(url, queueName string) *Mq {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
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

	mq := &Mq{
		Conn:        conn,
		Channel:     ch,
		Queue:       queueName,
		retryBuffer: make(chan []byte, 100),
	}

	go mq.retryLoop()

	return mq
}

func (m *Mq) Publish(payload []byte) error {
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
		log.Println(err)
		select {
		case m.retryBuffer <- payload:
		default:
			log.Println(err)
		}
		return err
	}
	return nil
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
