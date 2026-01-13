package mq

import (
	"context"
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

			log.Println("RabbitMQ connected successfully")

			mq := &Mq{
				Conn:        conn,
				Channel:     ch,
				Queue:       queue,
				confirms:    ch.NotifyPublish(make(chan amqp.Confirmation, 100)),
				retryBuffer: make(chan []byte, 100),
			}
			// запускаем retryLoop для сообщений, которые не удалось отправить
			go mq.retryLoop(ctx)

			return mq
		}
	}
}

func (m *Mq) Publish(payload []byte) error {
	if m.Channel == nil {
		log.Println("Publish failed: channel is nil, putting message to retryBuffer")
		select {
		case m.retryBuffer <- payload:
		default:
			log.Println("retryBuffer full, dropping message")
		}
		return nil
	}

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
		log.Printf("Publish error, pushing to retryBuffer: %v", err)
		select {
		case m.retryBuffer <- payload:
		default:
			log.Println("retryBuffer full, dropping message")
		}
		return err
	}

	return nil
}

// retryLoop повторно отправляет сообщения из retryBuffer
func (m *Mq) retryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("retryLoop stopped")
			return
		case msg := <-m.retryBuffer:
			if err := m.Publish(msg); err != nil {
				// если не удалось, возвращаем в очередь через 2 сек
				go func(msgCopy []byte) {
					time.Sleep(2 * time.Second)
					select {
					case m.retryBuffer <- msgCopy:
					default:
						log.Println("retryBuffer full, dropping message")
					}
				}(msg)
			} else {
				log.Println("retry successful")
			}
		}
	}
}

// Close закрывает соединение и канал
func (m *Mq) Close() {
	if m.Channel != nil {
		m.Channel.Close()
	}
	if m.Conn != nil {
		m.Conn.Close()
	}
}
