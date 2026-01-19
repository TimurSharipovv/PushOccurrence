package mq

import (
	"context"
	"log"
)

func CreateMq(ctx context.Context, url, queue string) *Mq {
	mq := &Mq{
		Buffer:   make(chan []byte, 100),
		Messages: make(chan []byte, 100),
		Connect:  make(chan bool, 1),
	}

	go mq.monitor(ctx)
	go mq.connectManager(ctx, url)
	go mq.messageManager(ctx)

	return mq
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
