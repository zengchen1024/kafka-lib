package agent

import (
	"errors"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

var (
	mqInstance mq.MQ
	subscriber *serviceImpl
	publisher  *publisherImpl
)

func Init(cfg *Config, log mq.Logger, redis Redis) error {
	if log == nil {
		return errors.New("missing log")
	}

	v := kafka.NewMQ(
		mq.Addresses(cfg.mqConfig().Addresses...),
		mq.Log(log),
	)

	if err := v.Init(); err != nil {
		return err
	}

	if err := v.Connect(); err != nil {
		return err
	}

	mqInstance = v
	subscriber = &serviceImpl{logger: log}

	newPublisher(redis, log)

	return nil
}

func Exit() {
	if subscriber != nil {
		subscriber.unsubscribe()

		subscriber = nil
	}

	if publisher != nil {
		publisher.exit()

		publisher = nil
	}

	if mqInstance != nil {
		if err := mqInstance.Disconnect(); err != nil {
			mqInstance.Options().Log.Errorf("exit kafka, err:%v", err)
		}

		mqInstance = nil
	}
}

func Subscribe(group string, h Handler, topics []string) error {
	if group == "" || h == nil || len(topics) == 0 {
		return errors.New("missing parameters")
	}

	if subscriber == nil {
		return errors.New("unimplemented")
	}

	return subscriber.subscribe(
		h, topics,
		mq.Queue(group),
		mq.SubscribeStrategy(mq.StrategyDoOnce),
	)
}

func SubscribeWithStrategyOfRetry(group string, h Handler, topics []string, retryNum int) error {
	if group == "" || h == nil || len(topics) == 0 || retryNum == 0 {
		return errors.New("missing parameters")
	}

	if subscriber == nil {
		return errors.New("unimplemented")
	}

	return subscriber.subscribe(
		h, topics,
		mq.Queue(group),
		mq.SubscribeRetryNum(retryNum),
		mq.SubscribeStrategy(mq.StrategyRetry),
	)
}

func SubscribeWithStrategyOfSendBack(group string, h Handler, topics []string) error {
	if group == "" || h == nil || len(topics) == 0 {
		return errors.New("missing parameters")
	}

	if subscriber == nil {
		return errors.New("unimplemented")
	}

	return subscriber.subscribe(
		h, topics,
		mq.Queue(group),
		mq.SubscribeStrategy(mq.StrategySendBack),
	)
}

// Handler
type Handler func([]byte, map[string]string) error

// serviceImpl
type serviceImpl struct {
	subscribers []mq.Subscriber
	logger      mq.Logger
}

func (impl *serviceImpl) unsubscribe() {
	s := impl.subscribers
	for i := range s {
		if err := s[i].Unsubscribe(); err != nil {
			impl.logger.Errorf(
				"failed to unsubscribe to topic:%v, err:%v",
				s[i].Topics(), err,
			)
		}
	}
}

func (impl *serviceImpl) subscribe(h Handler, topics []string, opts ...mq.SubscribeOption) error {
	s, err := mqInstance.Subscribe(
		func(e mq.Event) error {
			msg := e.Message()
			if msg == nil {
				return nil
			}

			return h(msg.Body, msg.Header)
		},
		topics,
		opts...,
	)
	if err == nil {
		impl.subscribers = append(impl.subscribers, s)
	}

	return err
}
