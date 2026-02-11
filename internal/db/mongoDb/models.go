package mongoDb

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OutboxMessage struct {
	Id         primitive.ObjectID `bson:"_id,omitempty"`
	Topic      string             `bson:"topic"`
	Payload    []byte             `bson:"payload"`
	Status     string             `bson:"status"`
	RetryCount int                `bson:"retryCount"`
	LastError  *string            `bson:"last_error,omitempty"`
	Attempts   int                `bson:"attempts"`
	CreatedAt  time.Time          `bson:"createdAt"`
	UpdatedAt  time.Time          `bson:"updatedAt"`
}
