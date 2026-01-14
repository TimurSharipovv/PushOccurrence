package mq

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mq struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	Queue       string
	retryBuffer chan []byte
	confirms    <-chan amqp.Confirmation
	connected   bool
	mu          sync.RWMutex
}
