package mq

import (
	"context"
	"log"

	"PushOccurrence/internal/db/mongoDb"
)

func (mq *Mq) MessageManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("messageManger stopping")
			return
		case connected := <-mq.RePublishStatus:
			if connected {
				mq.cleaningBuffer()
			}
		case msg := <-mq.Messages:
			mq.mutex.RLock()
			ch := mq.Channel
			conn := mq.Conn
			mq.mutex.RUnlock()
			if conn != nil && ch != nil && !conn.IsClosed() {
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
	if mq.Channel == nil {
		log.Println("have no connection")
		mq.sendToBuffer(msg)
		return
	}

	mq.Publish(msg)
}

func (mq *Mq) cleaningBuffer() {
	for {
		select {
		case msg := <-mq.Buffer:
			if mq.Channel == nil {
				log.Println("have no connection, write to buffer")
				return
			}

			mq.Publish(msg)
		default:
			return
		}
	}
}

func SendToOutbox(ctx context.Context, msg Message, repo mongoDb.OutboxRepositoryInteface) error {
	outboxMsg := mongoDb.OutboxMessage{
		Payload: msg.Payload,
		Topic:   "failed_rabbit_msg",
	}

	_, err := repo.Insert(ctx, outboxMsg)
	return err
}
