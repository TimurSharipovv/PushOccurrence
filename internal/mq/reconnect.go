package mq

/* import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Reconnect(ctx context.Context, url string) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		default:
			conn, err = amqp.Dial(url)
			if err == nil {
				log.Println("succesfully connect to mq")
				return conn, nil
			}

			log.Printf("mq unavailanle, retry after 30 seconds %v", err)

			time.Sleep(30 * time.Second)
		}
	}
}
*/
