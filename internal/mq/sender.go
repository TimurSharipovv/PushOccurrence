package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (mq *Mq) messageManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case connected := <-mq.Connect:
			if connected {
				mq.cleaningBuffer()
			}
		case msg := <-mq.Messages:
			if mq.Conn != nil && !mq.Conn.IsClosed() {
				mq.sendToRabbit(msg)
			} else {
				mq.sendToBuffer(msg)
			}
		}
	}
}

func (mq *Mq) sendToBuffer(msg Message) {
	for {
		select {
		case mq.Buffer <- msg:
			log.Printf("message write to buffer successfully")
			return
		default:
			log.Println("buffer full")
			return
		}
	}
}

func (mq *Mq) sendToRabbit(msg Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if mq.Channel == nil {
		log.Println("have no connection")
		mq.sendToBuffer(msg)
		return
	}

	confirmation, err := mq.Channel.PublishWithDeferredConfirmWithContext(ctx,
		"",
		mq.Queue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         msg.Payload,
		},
	)

	if err != nil {
		log.Printf("Publish error: %v", err)
		mq.sendToBuffer(msg)
		return
	}

	if confirmation == nil {
		log.Printf("confirmation is nil %v", err)
	}

	ok, err := confirmation.WaitContext(ctx)

	if err != nil {
		log.Printf("Confirmation timeout/error: %v", err)
		mq.sendToBuffer(msg)
	}

	if !ok {
		log.Println("Message NACKed by broker")
		mq.sendToBuffer(msg)
	} else {
		log.Println("Message delivered and confirmed")
	}
}

func (mq *Mq) cleaningBuffer() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case msg := <-mq.Buffer:
			if mq.Channel == nil {
				log.Println("have no connection, write to buffer")
				return
			}

			confirmation, err := mq.Channel.PublishWithDeferredConfirmWithContext(ctx,
				"",
				mq.Queue,
				false,
				false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					ContentType:  "application/json",
					Body:         msg.Payload,
				},
			)

			if err != nil {
				log.Printf("Publish error: %v", err)
				mq.sendToBuffer(msg)
				return
			}

			if confirmation == nil {
				log.Printf("confirmation is nil %v", err)
			}

			ok, err := confirmation.WaitContext(ctx)

			if err != nil {
				log.Printf("Confirmation timeout/error: %v", err)
				mq.sendToBuffer(msg)
			}

			if !ok {
				log.Println("Message NACKed by broker")
				mq.sendToBuffer(msg)
			} else {
				log.Println("Message delivered and confirmed")
			}
		default:
			return
		}
	}
}
