package jetstream

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type Subscriber interface {
	Subscribe(subj string, handler nats.MsgHandler) error
	SubscribeSync(subj string) error
}

type SubscriberConfig struct {
	RawConnectionConfig
	Handler func(*nats.Msg)
}

type JetStreamSubscriber struct {
	conn    nats.JetStreamContext
	logger  watermill.LoggerAdapter
	options []nats.SubOpt
}

func NewSubscriber(config *SubscriberConfig, logger watermill.LoggerAdapter, options ...nats.SubOpt) (Subscriber, error) {
	js, err := NewRawConnection(&config.RawConnectionConfig)
	if err != nil {
		return nil, err
	}

	return &JetStreamSubscriber{
		conn:    js,
		logger:  logger,
		options: options,
	}, nil
}

func (s *JetStreamSubscriber) Subscribe(subj string, handler nats.MsgHandler) error {
	if handler == nil {
		return errors.New("subscription messages handler can't be nil")
	}

	if _, err := s.conn.Subscribe(subj, handler, s.options...); err != nil {
		return err
	}

	return nil
}

func (s *JetStreamSubscriber) SubscribeSync(subj string) error {
	_, err := s.conn.SubscribeSync(subj, s.options...)
	if err != nil {
		return err
	}

	return nil
}
