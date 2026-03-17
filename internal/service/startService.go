package service

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	// mongoDb "PushOccurrence/internal/db/mongo"
	"PushOccurrence/internal/db/pg"
	"PushOccurrence/internal/handlers"
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
	mqConnStr := BuildMQConnString(cfg)
	// mongoConnStr := BuildMongoConnString(cfg)

	// mongoClient, err := mongoDb.Connect(ctx, mongoConnStr)
	// if err != nil {
	// 	log.Fatalf("failed to connect to mongo %v", err)
	// }
	// defer func() {
	// 	log.Println("closing mongo conn")
	// 	err := mongoClient.Disconnect(ctx)
	// 	if err != nil {
	// 		log.Println("err diconnect mongo")
	// 	}
	// 	log.Println("mongo conn closed successfully")
	// }()
	// log.Println("connect success")

	// repo := mongoDb.NewOutboxRepository(mongoClient.Database(cfg.Mongo.Database))
	// log.Println("init repo successfully")

	pg.Init(ctx, pgConnStr)
	defer func() {
		log.Println("closing pg")
		pg.Close()
		log.Println("closed pg successfully")
	}()
	log.Println("success init pg conn")

	listenConn := pg.AcquireConn(ctx)
	defer func() {
		log.Println("releasing listenConn")
		listenConn.Release()
	}()

	pg.ListenChannels(ctx, listenConn, cfg.Listener.Channels)

	rabbit := mq.InitMq(ctx, mqConnStr, cfg.RabbitMQ.Queue.Name)
	defer rabbit.Close()

	pendingIDs, err := pg.FetchPendingMessages(ctx, pg.Pool)
	if err != nil {
		log.Printf("failed to fetch pending messages: %v", err)
	} else {
		log.Printf("found %d pending messages, starting processing...", len(pendingIDs))
		for _, id := range pendingIDs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				handlers.HandleMessage(ctx, pg.Pool, rabbit, id)
			}()
		}
	}

	log.Println("service started, waiting for notifications...")

	notifyCh := make(chan *pgconn.Notification)

	wg.Add(1)
	go func() {
		defer wg.Done()
		pg.RunListener(ctx, pg.Pool, cfg.Listener.Channels, notifyCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pg.MainLoop(ctx, notifyCh, sigCh, rabbit, cancel)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// StartPoller(ctx, repo, rabbit)
	}()

	<-ctx.Done()
	log.Println("shutdown started, waiting for goroutines...")

	wg.Wait()
	log.Println("service stopped gracefully")
}
