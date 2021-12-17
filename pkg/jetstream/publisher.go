package jetstream

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

const (
	defaultWorkersCount   = 100
	defaultPublishTimeout = 5 * time.Second
)

type Publisher interface {
	Publish(subj string, msg ...*message.Message) error
	PublishAsync(subj string, msg ...*message.Message) error
}

type PublisherConfig struct {
	RawConnectionConfig
	WorkersCount int
	AsyncTimeout time.Duration
}

type JetStreamPublisher struct {
	conn         nats.JetStreamContext
	logger       watermill.LoggerAdapter
	options      []nats.PubOpt
	workersCount int
	asyncTimeout time.Duration
}

func NewPublisher(config *PublisherConfig, logger watermill.LoggerAdapter, options ...nats.PubOpt) (Publisher, error) {
	js, err := NewRawConnection(&config.RawConnectionConfig)
	if err != nil {
		return nil, err
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"ORDERS.*"},
	}); err != nil {
		return nil, err
	}

	if config.AsyncTimeout == 0 {
		config.AsyncTimeout = defaultPublishTimeout
	}

	if config.WorkersCount == 0 {
		config.WorkersCount = defaultWorkersCount
	}

	return &JetStreamPublisher{
		conn:         js,
		logger:       logger,
		options:      options,
		workersCount: config.WorkersCount,
		asyncTimeout: config.AsyncTimeout,
	}, nil
}

func (p *JetStreamPublisher) Publish(subj string, msg ...*message.Message) error {
	for _, m := range msg {
		var buf bytes.Buffer

		if err := json.NewEncoder(&buf).Encode(m); err != nil {
			return errors.Wrap(err, "can't encode message to json")
		}

		if _, err := p.conn.Publish(subj, buf.Bytes(), p.options...); err != nil {
			return errors.Wrap(err, "can't publish synchronously to JetStream")
		}
	}

	return nil
}

func (p *JetStreamPublisher) PublishAsync(subj string, msg ...*message.Message) error {
	for _, m := range msg {
		var buf bytes.Buffer

		if err := json.NewEncoder(&buf).Encode(m); err != nil {
			return errors.Wrap(err, "can't encode to json")
		}

		for i := 0; i < p.workersCount; i++ {
			if _, err := p.conn.PublishAsync(subj, buf.Bytes(), p.options...); err != nil {
				return errors.Wrap(err, "can't publish asynchronously to JetStream")
			}
		}

		select {
		case <-p.conn.PublishAsyncComplete():
		case <-time.After(p.asyncTimeout):
			return errors.New("didn't resolve in time")
		}
	}

	return nil
}
