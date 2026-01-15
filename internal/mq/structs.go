package mq

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mq struct {
	Conn         *amqp.Connection
	Channel      *amqp.Channel
	Queue        string
	URL          string
	Messages     chan []byte
	Buffer       chan []byte
	Connect      chan bool
	PublishMutex sync.Mutex
}
