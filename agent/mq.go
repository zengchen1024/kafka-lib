package agent

import (
	"errors"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/agent/publisher"
	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

var (
	instance *serviceImpl
	Publish  = publisher.Publish
)

func Init(cfg *Config, log *logrus.Entry, redis publisher.Redis) error {
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

	publisher.Init(redis)

	instance = &serviceImpl{}

	return nil
}

func Exit() {
	if instance != nil {
		instance.unsubscribe()

		instance = nil
	}

	publisher.Exit()

	if err := kafka.Disconnect(); err != nil {
		logrus.Errorf("exit kafka, err:%v", err)
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
			logrus.Errorf(
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
