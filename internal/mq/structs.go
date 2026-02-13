package mq

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mq struct {
	Conn            *amqp.Connection
	Channel         *amqp.Channel
	Queue           string
	URL             string
	Messages        chan Message
	Buffer          chan Message
	ConnectStatus   chan bool
	RePublishStatus chan bool
	Connected       bool

	PublishMutex sync.Mutex
	mutex        sync.RWMutex
}

type Message struct {
	MessageId string
	Payload   []byte
}
