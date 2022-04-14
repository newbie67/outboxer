package pubsub_test

import (
	"context"
	"fmt"
	"os"

	pubsubraw "cloud.google.com/go/pubsub"
	"gitlab.b2bdev.pro/backend/go-packages/outboxer-lib/es/pubsub"
)

func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pubsubraw.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"))
	if err != nil {
		fmt.Printf("failed to connect to gcp: %s", err)
		return
	}

	es := pubsub.New(client)

	// this is done internally by outboxer
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			pubsub.TopicNameOption:   "test",
			pubsub.OrderingKeyOption: "order",
		},
	}); err != nil {
		fmt.Printf("an error was not expected: %s", err)
		return
	}
}
