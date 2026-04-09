package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	queue      amqp.Queue
	conn       *amqp.Connection
	channel    *amqp.Channel
	endChannel chan bool
}

// Close implements [middleware.Middleware].
func (e *Exchange) Close() error {
	// if e.endChannel != nil {
	// 	e.endChannel <- true
	// }
	if e.channel != nil {
		err := e.channel.Close()
		println("LLEGA ACA!")
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}
	if e.conn != nil {
		err := e.conn.Close()
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
	}
	return nil
}

// Send implements [middleware.Middleware].
func (e *Exchange) Send(msg m.Message) (err error) {
	if e.channel == nil {
		return m.ErrMessageMiddlewareMessage
	}
	e.channel.Publish(
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	return nil
}

// StartConsuming implements [middleware.Middleware].
func (e *Exchange) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	e.conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		e.conn.Close()
		return m.ErrMessageMiddlewareMessage
	}

	e.channel, err = e.conn.Channel()
	if err != nil {
		e.channel.Close()
		return m.ErrMessageMiddlewareMessage
	}

	err = e.channel.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	e.queue, err = e.channel.QueueDeclare(
		"queuename", // name
		false,       // durability
		false,       // delete when unused
		true,        // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	err = e.channel.QueueBind(
		"queuename", // queue name
		"",          // routing key
		"logs",      // exchange
		false,
		nil)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	print("LLEGA EL STARTCONSUMING")
	// e.endChannel = make(chan bool)
	go func() error {
		for {
			msgs, err := e.channel.Consume(
				"queuename", // queue
				"",          // consumer
				false,       // auto-ack
				false,       // exclusive
				false,       // no-local
				false,       // no-wait
				nil,         // args
			)
			if err != nil {
				return m.ErrMessageMiddlewareDisconnected
			}
			for d := range msgs {
				msg := m.Message{Body: string(d.Body)}
				ack := func() { d.Ack(false) }
				nack := func() { d.Nack(false, false) }
				callbackFunc(msg, ack, nack)
			}
		}
	}()
	return nil
}

// StopConsuming implements [middleware.Middleware].
func (e *Exchange) StopConsuming() {
	// if e.endChannel != nil {
	// 	e.endChannel <- true
	// 	e.channel.Close()
	// 	e.conn.Close()
	// }
	e.channel.Close()
	e.conn.Close()
}
