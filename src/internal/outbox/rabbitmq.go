package outbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const operationTimeout = 5 * time.Second

// One worker owns this connection and allows one unconfirmed publish at a time.
// ponytail: serial confirms bound memory. Add a bounded in-flight window if
// measured throughput requires concurrent publishing.
type broker struct {
	extraQueues []string
	url         string
	prefix      string
	socket      net.Conn
	channel     *amqp.Channel
	confirms    <-chan amqp.Confirmation
	returns     <-chan amqp.Return
}

func (b *broker) close() {
	if b.socket != nil {
		// Closing the transport also unblocks RPCs and publish writes.
		b.socket.Close()
	}
	b.socket = nil
	b.channel = nil
}

func (b *broker) connect(ctx context.Context) error {
	if b.channel != nil && !b.channel.IsClosed() {
		return nil
	}
	b.close()
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	stopClose := func() bool {
		return true
	}
	connection, err := amqp.DialConfig(b.url, amqp.Config{
		Heartbeat: 5 * time.Second,
		Dial: func(network, address string) (net.Conn, error) {
			socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			b.socket = socket
			stopClose = context.AfterFunc(ctx, func() { socket.Close() })
			return socket, nil
		},
	})
	defer stopClose()
	if err != nil {
		b.close()
		// The connection URL may contain credentials; never persist it in errors.
		return errors.New("RabbitMQ connection failed")
	}
	channel, err := connection.Channel()
	if err == nil {
		b.channel = channel
		for _, name := range append([]string{"feed", "search"}, b.extraQueues...) {
			queue := b.prefix + "." + name
			_, err = channel.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "classic"})
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		err = channel.Confirm(false)
	}
	if err != nil {
		b.close()
		return fmt.Errorf("prepare RabbitMQ channel: %w", err)
	}
	b.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	b.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil
}

func (b *broker) publish(ctx context.Context, queue string, eventID int64, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	socket := b.socket
	// PublishWithContext in amqp091-go v1.10 does not cancel a blocked write.
	// Closing the transport bounds writes and makes their uncertain outcome retryable.
	stopClose := context.AfterFunc(ctx, func() { socket.Close() })
	defer stopClose()
	err := b.channel.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    strconv.FormatInt(eventID, 10),
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish to %s: %w", queue, err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case confirmation, ok := <-b.confirms:
		if !ok {
			return errors.New("RabbitMQ confirmation channel closed")
		}
		// RabbitMQ sends basic.return before confirm for unroutable messages.
		// The client dispatches return into this buffered channel before confirm.
		select {
		case returned, ok := <-b.returns:
			if !ok {
				return errors.New("RabbitMQ return channel closed")
			}
			return fmt.Errorf("unroutable queue %s: %d %s", queue, returned.ReplyCode, returned.ReplyText)
		default:
		}
		if !confirmation.Ack {
			return errors.New("RabbitMQ negatively acknowledged publication")
		}
		return nil
	}
}
