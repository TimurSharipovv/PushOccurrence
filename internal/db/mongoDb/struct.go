package mongoDb

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OutboxMessage struct {
	Id          primitive.ObjectID `bson:"_id,omitempty"`
	Topic       string             `bson:"topic"`
	Payload     []byte             `bson:"payload"`
	Status      string             `bson:"status"`
	RetryCount  int                `bson:"retryCount"`
	CreatedAt   time.Time          `bson:"createdAt"`
	UpdatedAt   time.Time          `bson:"updatedAt"`
	NextRetryAt time.Time          `bson:"nextRetryAt"`
}
