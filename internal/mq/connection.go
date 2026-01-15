package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (mq *Mq) connect(rabbitURL string) error {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	_, err = ch.QueueDeclare(
		mq.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	mq.Conn = conn
	mq.Channel = ch
	log.Println("MQ connected successfully")
	return nil
}

func (mq *Mq) monitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if mq.Conn == nil || mq.Conn.IsClosed() {
				select {
				case mq.Connect <- false:
				default:
				}
			} else {
				select {
				case mq.Connect <- true:
				default:
				}
			}
		}
	}
}

func (mq *Mq) connectManager(ctx context.Context, url string) {
	for {
		select {
		case <-ctx.Done():
			return
		case connected := <-mq.Connect:
			if !connected {
				log.Println("Attempting reconnect...")
				if err := mq.connect(url); err != nil {
					log.Printf("Reconnect failed: %v", err)
				}
			}
		}
	}
}

func (mq *Mq) Close() {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()

	if mq.Channel != nil {
		_ = mq.Channel.Close()
	}

	if mq.Conn != nil {
		_ = mq.Conn.Close()
	}
}
