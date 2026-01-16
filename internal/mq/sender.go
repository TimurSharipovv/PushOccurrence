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
		log.Printf("publish failed %v", err)
		mq.sendToBuffer(payload)
		return
	}
}

func (mq *Mq) sendToBuffer(payload []byte) {
	for {
		select {
		case mq.Buffer <- payload:
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
	for {
		select {
		case payload := <-mq.Buffer:
			mq.sendToRabbit(payload)
		default:
			return
		}
	}
}
