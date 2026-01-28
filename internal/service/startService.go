package service

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"PushOccurrence/internal/db"
	"PushOccurrence/internal/mq"

	"github.com/jackc/pgx/v5/pgconn"
)

func StartService(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var wg sync.WaitGroup

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer func() {
		signal.Stop(sigCh)
		close(sigCh)
	}()

	cfg := LoadConfig("config/config.json")

	pgConnStr := BuildConnString(cfg)

	db.Init(ctx, pgConnStr)
	defer func() {
		log.Println("closing db")
		db.Close()
		log.Println("db closed")
	}()

	listenConn := db.AcquireConn(ctx)
	defer func() {
		log.Println("releasing listenConn")
		listenConn.Release()
	}()

	db.ListenChannels(ctx, listenConn, cfg.Listener.Channels)

	rabbit := mq.CreateMq(ctx, cfg.RabbitMQ.URL, cfg.RabbitMQ.Queue.Name)
	defer rabbit.Close()

	log.Println("service started, waiting for notifications...")

	notifyCh := make(chan *pgconn.Notification)

	wg.Add(1)
	go func() {
		defer wg.Done()
		db.ListenNotifications(ctx, listenConn, notifyCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		db.MainLoop(ctx, notifyCh, sigCh, rabbit, cancel)
	}()

	<-ctx.Done()
	log.Println("shutdown started, waiting for goroutines...")

	wg.Wait()
	log.Println("service stopped gracefully")
}
