package mq

import (
	"context"
)

func CreateMq(ctx context.Context, url, queue string) *Mq {
	mq := &Mq{
		Buffer:          make(chan Message, 100),
		Messages:        make(chan Message, 100),
		ConnectStatus:   make(chan bool, 1),
		RePublishStatus: make(chan bool, 1),
	}

	go mq.Monitor(ctx)
	go mq.connectManager(ctx, url)
	go mq.MessageManager(ctx)

	return mq
}
