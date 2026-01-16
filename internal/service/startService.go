package service

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"PushOccurrence/internal/db"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5/pgconn"
)

func StartService() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	cfg := LoadConfig("config/config.json")

	pgConnStr := BuildConnString(cfg)

	db.Init(ctx, pgConnStr)
	defer db.Close(ctx)

	listenConn := AcquireConn(ctx)

	ListenChannels(ctx, listenConn, cfg.Listener.Channels)

	rabbit := mq.CreateMq(ctx, cfg.RabbitMQ.URL, cfg.RabbitMQ.Queue.Name)
	defer rabbit.Close()

	log.Println("service started, waiting for notifications...")

	notifyCh := make(chan *pgconn.Notification)

	go ListenNotifications(ctx, listenConn, notifyCh)

	MainLoop(ctx, notifyCh, sigCh, rabbit)
}
