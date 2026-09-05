package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	newCh, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range newCh {
			var val T
			err := json.Unmarshal(d.Body, &val)
			if err != nil {
				return
			}
			handler(val)
			d.Ack(false) // Acknowledge the message after processing
		}
	}()
	return nil
}
