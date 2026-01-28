package main

import (
	"PushOccurrence/internal/service"
	"context"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.StartService(ctx)
}
