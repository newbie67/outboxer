package amqp_test

import (
	"context"
	"fmt"
	"os"

	"gitlab.b2bdev.pro/backend/go-packages/outboxer-lib"
	amqpOut "gitlab.b2bdev.pro/backend/go-packages/outboxer-lib/es/amqp"
)

func ExampleNewAMQP() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := amqp.Dial(os.Getenv("ES_DSN"))
	if err != nil {
		fmt.Printf("failed to connect to amqp: %s", err)
		return
	}

	defer conn.Close()

	es := amqpOut.NewAMQP(conn)

	// this is done internally by outboxer
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqpOut.ExchangeNameOption: "test",
			amqpOut.ExchangeTypeOption: "topic",
			amqpOut.RoutingKeyOption:   "test.send",
		},
	}); err != nil {
		fmt.Printf("an error was not expected: %s", err)
		return
	}
}
