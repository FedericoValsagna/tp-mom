package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	queue      amqp.Queue
	conn       *amqp.Connection
	channel    *amqp.Channel
	name       string
	endChannel chan bool
}

// Close implements [middleware.Middleware].
func (q *Queue) Close() error {
	if q.channel != nil {
		err := q.channel.Close()
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}
	if q.conn != nil {
		err := q.conn.Close()
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}
	return nil
}

// Send implements [middleware.Middleware].
func (q *Queue) Send(msg m.Message) (err error) {
	if q.channel == nil {
		return m.ErrMessageMiddlewareMessage
	}
	err = q.channel.Publish(
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	// log.Printf(" [x] Sent %s\n", msg.Body)
	return nil
}

// StartConsuming implements [middleware.Middleware].
func (q *Queue) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	q.name = "Nombredequeue"

	q.conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		// q.conn.Close()
		return m.ErrMessageMiddlewareMessage
	}

	q.channel, err = q.conn.Channel()

	if err != nil {
		// q.channel.Close()
		return m.ErrMessageMiddlewareMessage
	}

	q.queue, err = q.channel.QueueDeclare(
		q.name, // name
		true,   // durability
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	msgs, err := q.channel.Consume(
		q.queue.Name, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}
	q.endChannel = make(chan bool)
	go func() {
		for d := range msgs {
			msg := m.Message{Body: string(d.Body)}
			ack := func() { d.Ack(false) }
			nack := func() { d.Nack(false, false) }
			callbackFunc(msg, ack, nack)
		}
	}()
	<-q.endChannel
	return
}

// StopConsuming implements [middleware.Middleware].
func (q *Queue) StopConsuming() {
	if q.endChannel != nil {
		q.endChannel <- true
	}
}
