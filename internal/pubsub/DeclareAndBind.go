package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	durable SimpleQueueType = iota
	transient

	SimpleQueueTypeDurable   = durable
	SimpleQueueTypeTransient = transient
)

func (sqt SimpleQueueType) String() string {
	switch sqt {
	case durable:
		return "durable"
	case transient:
		return "transient"
	default:
		return "unknown"
	}
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	connChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	amqpQueue, err := connChannel.
		QueueDeclare(queueName, queueType == durable,
			queueType == transient,
			queueType == transient,
			false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	connChannel.QueueBind(amqpQueue.Name, key, exchange, false, nil)
	return connChannel, amqpQueue, nil
}
