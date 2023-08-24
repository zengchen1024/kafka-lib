package agent

import (
	"errors"

	"github.com/opensourceways/kafka-lib/agent/publisher"
	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

var (
	instance *serviceImpl
	Publish  = publisher.Publish
	logger   mq.Logger
)

func Init(cfg *Config, log mq.Logger, redis publisher.Redis) error {
	if log == nil {
		return errors.New("missing log")
	}

	err := kafka.Init(
		mq.Addresses(cfg.mqConfig().Addresses...),
		mq.Log(log),
	)
	if err != nil {
		return err
	}

	if err := kafka.Connect(); err != nil {
		return err
	}

	publisher.Init(redis, log)

	instance = &serviceImpl{}
	logger = log

	return nil
}

func Exit() {
	if instance != nil {
		instance.unsubscribe()

		instance = nil
	}

	publisher.Exit()

	if err := kafka.Disconnect(); err != nil {
		logger.Errorf("exit kafka, err:%v", err)
	}
}

func Subscribe(group string, h Handler, topics []string) error {
	if instance == nil {
		return errors.New("unimplemented")
	}

	if group == "" || h == nil || len(topics) == 0 {
		return errors.New("missing parameters")
	}

	return instance.subscribe(group, h, topics)
}

// Handler
type Handler func([]byte, map[string]string) error

// serviceImpl
type serviceImpl struct {
	subscribers []mq.Subscriber
}

func (impl *serviceImpl) unsubscribe() {
	s := impl.subscribers
	for i := range s {
		if err := s[i].Unsubscribe(); err != nil {
			logger.Errorf(
				"failed to unsubscribe to topic:%v, err:%v",
				s[i].Topics(), err,
			)
		}
	}
}

func (impl *serviceImpl) subscribe(group string, h Handler, topics []string) error {
	s, err := impl.registerHandler(group, h, topics)
	if err == nil {
		impl.subscribers = append(impl.subscribers, s)
	}

	return err
}

func (impl *serviceImpl) registerHandler(group string, h Handler, topics []string) (mq.Subscriber, error) {
	return kafka.Subscribe(
		group,
		func(e mq.Event) error {
			msg := e.Message()
			if msg == nil {
				return nil
			}

			return h(msg.Body, msg.Header)
		},
		topics,
	)
}
