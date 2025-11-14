package mq

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	url     string

	queueName string

	retryCh chan *amqp.Publishing
	wg      sync.WaitGroup
	closed  chan struct{}
}

func NewProducer(amqpURL, queueName string) (*Producer, error) {
	p := &Producer{
		url:       amqpURL,
		queueName: queueName,
		retryCh:   make(chan *amqp.Publishing, 1000),
		closed:    make(chan struct{}),
	}

	if err := p.connectAndSetup(); err != nil {
		return nil, err
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.retryLoop()
	}()

	return p, nil
}

func (p *Producer) connectAndSetup() error {
	if p.channel != nil {
		_ = p.channel.Close()
		p.channel = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}

	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}

	_, err = ch.QueueDeclare(
		p.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}

	p.conn = conn
	p.channel = ch

	go p.monitorConnection()

	log.Printf("MQ: connected to %s, queue=%s", p.url, p.queueName)
	return nil
}

func (p *Producer) monitorConnection() {
	confirmations := p.channel.NotifyPublish(make(chan amqp.Confirmation, 1000))
	closeErrChan := make(chan *amqp.Error)
	p.conn.NotifyClose(closeErrChan)

	for {
		select {
		case conf := <-confirmations:
			if !conf.Ack {
				log.Printf("MQ warn: message not acked by broker (tag=%d)", conf.DeliveryTag)
			}
		case amqErr := <-closeErrChan:
			if amqErr != nil {
				log.Printf("MQ connection closed: %v; will attempt reconnect", amqErr)
				for i := 0; ; i++ {
					select {
					case <-p.closed:
						return
					default:
					}
					wait := time.Second * time.Duration(1<<uint(min(i, 6))) // до ~64s
					time.Sleep(wait)
					if err := p.connectAndSetup(); err != nil {
						log.Printf("MQ reconnect failed: %v; retrying...", err)
						continue
					}
					log.Printf("MQ reconnected")
					return
				}
			}
			return
		case <-p.closed:
			return
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (p *Producer) Publish(ctx context.Context, body []byte, routingKey string) error {
	if p.channel == nil {
		return errors.New("mq: channel not ready")
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
	}

	if err := p.channel.PublishWithContext(ctx,
		"",
		p.queueName,
		false, false,
		msg,
	); err != nil {
		select {
		case p.retryCh <- &msg:
			log.Printf("MQ publish failed: %v — queued for retry", err)
		default:
			log.Printf("MQ publish failed and retry buffer full: %v", err)
			return err
		}
		return err
	}
	return nil
}

func (p *Producer) retryLoop() {
	for {
		select {
		case msg := <-p.retryCh:
			for i := 0; i < 8; i++ {
				if p.channel == nil {
					time.Sleep(time.Second * 2)
					continue
				}
				err := p.channel.Publish("", p.queueName, false, false, *msg)
				if err == nil {
					break
				}
				wait := time.Second * time.Duration(1<<uint(min(i, 6)))
				time.Sleep(wait)
			}
		case <-p.closed:
			return
		}
	}
}

func (p *Producer) Close() {
	close(p.closed)
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.wg.Wait()
}
