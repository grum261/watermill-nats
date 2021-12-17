package jetstream

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type RawConnectionConfig struct {
	Addr             string
	StreamName       string
	Subjects         []string
	NatsOptions      []nats.Option
	JetStreamOptions []nats.JSOpt
}

func NewRawConnection(config *RawConnectionConfig) (nats.JetStreamContext, error) {
	conn, err := nats.Connect(config.Addr, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "can't connect to NATS")
	}

	js, err := conn.JetStream(config.JetStreamOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "can't connect to JetStream")
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     config.StreamName,
		Subjects: config.Subjects,
	}, config.JetStreamOptions...); err != nil {
		return nil, err
	}

	return js, nil
}
