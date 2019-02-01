// Package amqp is the AMQP implementation of an event stream.
package amqp

import (
	"context"
	"fmt"

	"github.com/italolelis/outboxer"
	"github.com/streadway/amqp"
)

const (
	defaultExchangeType = "topic"

	// ExchangeNameOption is the exchange name option
	ExchangeNameOption = "exchange.name"

	// ExchangeTypeOption is the exchange type option
	ExchangeTypeOption = "exchange.type"

	// RoutingKeyOption is the routing key option
	RoutingKeyOption = "routing_key"
)

// AMQP is the wrapper for the AMQP library
type AMQP struct {
	conn *amqp.Connection
}

type options struct {
	exchange     string
	exchangeType string
	routingKey   string
	passive      bool
	durable      bool
	autoDelete   bool
	internal     bool
	noWait       bool
}

// NewAMQP creates a new instance of AMQP
func NewAMQP(conn *amqp.Connection) *AMQP {
	return &AMQP{conn: conn}
}

// Send sends the message to the event stream
func (r *AMQP) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	opts := r.parseOptions(evt.Options)
	if err := ch.ExchangeDeclare(
		opts.exchange,     // name
		opts.exchangeType, // type
		opts.durable,      // durable
		opts.autoDelete,   // auto-deleted
		opts.internal,     // internal
		opts.noWait,       // noWait
		nil,               // arguments
	); err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}

	if err = ch.Publish(
		opts.exchange,   // publish to an exchange
		opts.routingKey, // routing to 0 or more queues
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         evt.Payload,
			DeliveryMode: amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:     0,              // 0-9
			Headers:      amqp.Table(evt.Headers),
		},
	); err != nil {
		return fmt.Errorf("exchange publish: %s", err)
	}

	return nil
}

func (r *AMQP) parseOptions(opts outboxer.DynamicValues) *options {
	opt := options{exchangeType: defaultExchangeType, durable: true}

	if data, ok := opts[ExchangeNameOption]; ok {
		opt.exchange = data.(string)
	}

	if data, ok := opts[ExchangeTypeOption]; ok {
		opt.exchangeType = data.(string)
	}

	if data, ok := opts["exchange.durable"]; ok {
		opt.durable = data.(bool)
	}

	if data, ok := opts["exchange.auto_delete"]; ok {
		opt.autoDelete = data.(bool)
	}

	if data, ok := opts["exchange.internal"]; ok {
		opt.internal = data.(bool)
	}

	if data, ok := opts["exchange.no_wait"]; ok {
		opt.noWait = data.(bool)
	}

	if data, ok := opts[RoutingKeyOption]; ok {
		opt.routingKey = data.(string)
	}

	return &opt
}