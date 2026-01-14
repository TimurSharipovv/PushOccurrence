package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func NewMq(ctx context.Context, url, queue string) *Mq {
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error

	for {
		select {
		case <-ctx.Done():
			log.Println("NewMq cancelled before connection")
			return nil
		default:
			conn, err = amqp.Dial(url)
			if err != nil {
				log.Printf("RabbitMQ unavailable, retry after 30s: %v", err)
				time.Sleep(30 * time.Second)
				continue
			}

			ch, err = conn.Channel()
			if err != nil {
				log.Printf("Failed to open channel, retry after 30s: %v", err)
				conn.Close()
				time.Sleep(30 * time.Second)
				continue
			}

			_, err = ch.QueueDeclare(
				queue,
				true,  // durable
				false, // autoDelete
				false, // exclusive
				false, // noWait
				nil,   // args
			)
			if err != nil {
				log.Printf("Failed to declare queue, retry after 30s: %v", err)
				ch.Close()
				conn.Close()
				time.Sleep(30 * time.Second)
				continue
			}

			if err := ch.Confirm(false); err != nil {
				log.Printf("Failed to enable confirm mode, retry after 30s: %v", err)
				ch.Close()
				conn.Close()
				time.Sleep(30 * time.Second)
				continue
			}

			log.Println("mq connected successfully")

			mq := &Mq{
				Conn:        conn,
				Channel:     ch,
				Queue:       queue,
				confirms:    ch.NotifyPublish(make(chan amqp.Confirmation, 100)),
				retryBuffer: make(chan []byte, 100),
			}
			mq.connected.Store(true)
			go mq.retryLoop(ctx)

			return mq
		}
	}
}

// тестовый набор
type Producer interface {
	Publish(body []byte) error
	Close() error
}
