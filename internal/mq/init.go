package mq

import (
	"context"
)

func InitMq(ctx context.Context, url, queue string) *Mq {
	mq := &Mq{
		Queue:         queue,
		URL:           url,
		ConnectStatus: make(chan bool, 1),
	}

	go mq.Monitor(ctx)
	go mq.connectManager(ctx, url)

	return mq
}
