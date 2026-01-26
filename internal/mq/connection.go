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

func (mq *Mq) Monitor(ctx context.Context) {
	log.Println("monitor start")
	defer log.Println("monitor stop")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("monitor stopping")
			return
		case <-ticker.C:
			if mq.Channel == nil || mq.Conn.IsClosed() {
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
	log.Println("connect manger start")
	defer log.Println("connect manager stop")
	for {
		select {
		case <-ctx.Done():
			log.Println("connect stopping")
			return
		case connected := <-mq.Connect:
			if !connected {
				select {
				case <-ctx.Done():
					log.Println("connect stopping during reconnect wait")
					return
				case <-time.After(3 * time.Second):
				}

				log.Println("Attempting reconnect...")
				err := mq.connect(url)
				if err != nil {
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
