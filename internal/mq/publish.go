package mq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (mq *Mq) PublishSync(ctx context.Context, msg Message) error {
	mq.mutex.RLock()
	ch := mq.Channel
	mq.mutex.RUnlock()

	if ch == nil {
		return fmt.Errorf("rabbit channel is nil (disconnected)")
	}

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	confirmation, err := ch.PublishWithDeferredConfirmWithContext(pubCtx,
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
		return fmt.Errorf("publish error: %w", err)
	}

	ok, err := confirmation.WaitContext(pubCtx)
	if err != nil {
		return fmt.Errorf("wait confirmation error: %w", err)
	}
	if !ok {
		return fmt.Errorf("negative ack received")
	}

	return nil
}

/* func (mq *Mq) PublishAsync(msg Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	ok, err := confirmation.WaitContext(ctx)
	if err != nil || !ok {
		log.Printf("Confirmation timeout/error: %v", err)
		mq.sendToBuffer(msg)
		return
	}
} */
