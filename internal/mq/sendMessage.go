package mq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (mq *Mq) sendToRabbit(payload []byte) {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()

	if mq.Conn == nil || mq.Channel == nil {
		mq.sendToBuffer(payload)
		return
	}

	err := mq.Channel.Publish(
		"",
		mq.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		},
	)

	if err != nil {
		log.Printf("sendToRabbit: publish failed, buffering message: %v", err)
		mq.sendToBuffer(payload)
		return
	}
}

func (mq *Mq) sendToBuffer(payload []byte) {
	select {
	case mq.Buffer <- payload:
	default:
		log.Println("buffer is full, message dropped")
	}
}

func (mq *Mq) messageManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-mq.Messages:
			if mq.Conn != nil && !mq.Conn.IsClosed() {
				mq.cleaningBuffer()
				mq.sendToRabbit(payload)
			} else {
				mq.sendToBuffer(payload)
			}
		}
	}
}

func (mq *Mq) cleaningBuffer() {
	if len(mq.Buffer) == 0 {
		return
	}

	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()

	for {
		select {
		case payload := <-mq.Buffer:
			err := mq.Channel.Publish(
				"",
				mq.Queue,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        payload,
				},
			)

			if err != nil {
				log.Println("failed to flush buffer", err)
				mq.sendToBuffer(payload)
				return
			}

		default:
			return
		}
	}
}
