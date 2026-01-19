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

			confirmation, err := mq.Channel.PublishWithDeferredConfirmWithContext(ctx,
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

			if err != nil {
				log.Printf("Publish error: %v", err)
				mq.sendToBuffer(payload)
				return
			}

			// Ждем подтверждения от брокера (WaitContext блокирует выполнение до прихода ACK/NACK или таймаута)
			ok, err := confirmation.WaitContext(ctx)

			// Если err != nil (таймаут/отмена контекста) ИЛИ ok == false (NACK от брокера)
			if err != nil {
				log.Printf("Confirmation timeout/error: %v", err)
				mq.sendToBuffer(payload)
			} else if !ok {
				log.Println("Message NACKed by broker") // Брокер явно отказался принимать сообщение
				mq.sendToBuffer(payload)
			} else {
				// Сообщение гарантированно в очереди.
				log.Println("Message delivered and confirmed")
			}
		default:
			return
		}
	}
}

// Предварительное условие: При создании подключения/канала вы должны один раз вызвать:
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

// err := mq.Channel.Confirm(false)
// Если вы этого не сделаете, брокер не будет слать подтверждения.

func (mq *Mq) sendToRabbit(payload []byte) {
	//  есмли брокер не шлет Ack 5 секунд то считаем как ошибку
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if mq.Conn == nil || mq.Channel == nil || mq.Conn.IsClosed() {
		log.Println("have no connection, write to buffer")
		mq.sendToBuffer(payload)
		return
	}

	//  Используем PublishWithDeferredConfirmWithContext
	confirmation, err := mq.Channel.PublishWithDeferredConfirmWithContext(ctx,
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

	if err != nil {
		log.Printf("Publish error: %v", err)
		mq.sendToBuffer(payload)
		return
	}

	// Ждем подтверждения от брокера (WaitContext блокирует выполнение до прихода ACK/NACK или таймаута)
	ok, err := confirmation.WaitContext(ctx)

	// Если err != nil (таймаут/отмена контекста) ИЛИ ok == false (NACK от брокера)
	if err != nil {
		log.Printf("Confirmation timeout/error: %v", err)
		mq.sendToBuffer(payload)
	} else if !ok {
		log.Println("Message NACKed by broker") // Брокер явно отказался принимать сообщение
		mq.sendToBuffer(payload)
	} else {
		// Сообщение гарантированно в очереди.
		log.Println("Message delivered and confirmed")
	}
}

/* func (mq *Mq) sendToRabbit(payload []byte) {
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

	if err != nil {
		mq.sendToBuffer(payload)
		}
		}

	}
} */
