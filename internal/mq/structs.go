package mq

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mq struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	Queue       string
	retryBuffer chan []byte
	confirms    <-chan amqp.Confirmation
	connected   atomic.Bool
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
		log.Printf("Publish error, pushing to retryBuffer: %v", err)

		m.connected.Store(false)

		select {
		case m.retryBuffer <- payload:
		default:
			log.Println("retryBuffer full, dropping message")
		}

		return err
	}

	return nil
}

func (m *Mq) retryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("retryLoop stopped")
			return
		default:
			if !m.connected.Load() {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			select {
			case msg := <-m.retryBuffer:
				err := m.Channel.Publish(
					"",
					m.Queue,
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body:        msg,
					},
				)
				if err != nil {
					log.Println("retry publish failed:", err)
					m.connected.Store(false)

					time.Sleep(2 * time.Second)
					select {
					case m.retryBuffer <- msg:
					default:
						log.Println("retryBuffer full, dropping message")
					}
				}
			default:
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
