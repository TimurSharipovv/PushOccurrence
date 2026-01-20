package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (mq *Mq) connect(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		ch.Close()
		conn.Close()
		log.Printf("cant put ch to confirm mode %v", err)
		return err
	}
	_, err = ch.QueueDeclare(
		"test",
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
	log.Println("mq connected successfully; confirm mode enabled")
	return nil
}

func (mq *Mq) monitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if mq.Channel == nil || mq.Conn.IsClosed() {
				select {
				case mq.Connect <- false:
					log.Println("Connect == false")
				default:
				}
			} else {
				select {
				case mq.Connect <- true:
					log.Println("Connect == true")
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
