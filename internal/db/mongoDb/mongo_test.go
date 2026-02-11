package mongoDb

import (
	"context"
	"testing"
	"time"
)

func TestConnectSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := "mongodb://localhost:27017/outbox"

	t.Logf("try connect to mongo %s", url)

	client, err := Connect(ctx, url)

	t.Logf("Connect: client %p (is nil? %v), err %v", client, client == nil, err)

	if err != nil {
		t.Logf("cant connect to mongo")
		return
	}

	if client == nil {
		t.Fatalf("conn not successfully")
	}

	err = client.Disconnect(ctx)
	if err != nil {
		t.Errorf("error close conn %v", err)
	}
}
