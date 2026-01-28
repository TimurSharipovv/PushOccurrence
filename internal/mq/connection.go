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

	mq.mutex.Lock()
	mq.Conn = conn
	mq.Channel = ch
	mq.mutex.Unlock()

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

			mq.mutex.RLock()
			conn := mq.Conn
			ch := mq.Channel
			mq.mutex.RUnlock()

			connected := conn != nil && ch != nil && !conn.IsClosed()

			mq.mutex.Lock()
			connect := mq.Connected
			mq.Connected = connected
			mq.mutex.Unlock()

			if connect == connected {
				continue
			}

			select {
			case mq.ConnectStatus <- connected:
			default:
			}

			select {
			case mq.RePublishStatus <- connected:
			default:
			}

			if connected {
				log.Println("monitor: connection is UP")
			} else {
				log.Println("monitor: connection is DOWN")
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
		case connected := <-mq.ConnectStatus:
			if !connected {
				if ctx.Err() != nil {
					return
				}
				select {
				case <-ctx.Done():
					log.Println("connect stopping during reconnect wait")
					return
				case <-time.After(3 * time.Second):
					if ctx.Err() != nil {
						return
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
}

func (mq *Mq) Close() {
	mq.PublishMutex.Lock()
	defer mq.PublishMutex.Unlock()

	mq.mutex.Lock()
	if mq.Channel != nil {
		_ = mq.Channel.Close()
		mq.Channel = nil
	}
	if mq.Conn != nil {
		_ = mq.Conn.Close()
		mq.Conn = nil
	}
	mq.mutex.Unlock()
}
