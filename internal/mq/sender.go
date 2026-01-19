package mq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (mq *Mq) sendToRabbit(payload []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if mq.Conn == nil || mq.Channel == nil || mq.Conn.IsClosed() {
		log.Println("have no connection, write to buffer")
		mq.sendToBuffer(payload)
		return
	}

	err := mq.Channel.PublishWithContext(ctx,
		"",
		mq.Queue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         payload,
		},
	)

	failOnError(err, "failed to declare queue")
	mq.sendToBuffer(payload)
}

func (mq *Mq) sendToBuffer(payload []byte) {
	for {
		select {
		case mq.Buffer <- payload:
			log.Printf("message write to buffer successfully")
			return
		default:
			log.Println("buffer full")
			return
		}
	}
}

func (mq *Mq) messageManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case connected := <-mq.Connect:
			if connected {
				mq.cleaningBuffer()
			}
		case payload := <-mq.Messages:
			if mq.Conn != nil && !mq.Conn.IsClosed() {
				mq.sendToRabbit(payload)
			} else {
				mq.sendToBuffer(payload)
			}
		}
	}
}

func (mq *Mq) cleaningBuffer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case payload := <-mq.Buffer:
			if mq.Conn == nil || mq.Channel == nil || mq.Conn.IsClosed() {
				log.Println("have no connection, write to buffer")
				return
			}

			err := mq.Channel.PublishWithContext(ctx,
				"",
				mq.Queue,
				false,
				false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					ContentType:  "application/json",
					Body:         payload,
				},
			)

			failOnError(err, "failed to declare queue")
		default:
			return
		}
	}
}
