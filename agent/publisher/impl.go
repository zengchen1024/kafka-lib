package publisher

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/opensourceways/kafka-lib/kafka"
	"github.com/opensourceways/kafka-lib/mq"
)

const queueName = "kafka"

var instance *publisherImpl

func Init(redis Redis) {
	if redis != nil {
		instance = &publisherImpl{
			q:       &queueImpl{redis},
			stop:    make(chan struct{}),
			stopped: make(chan struct{}),
		}

		instance.start()
	}
}

func Exit() {
	if instance != nil {
		instance.exit()

		instance = nil
	}
}

func Publish(topic string, header map[string]string, msg []byte) error {
	v := &mq.Message{
		Header: header,
		Body:   msg,
	}

	if instance != nil {
		return instance.publish(topic, v)
	}

	return kafka.Publish(topic, v)
}

// queue
type queue interface {
	push(string, *message) error
	pop(string) (message, error)
	isEmpty(error) bool
}

// publisherImpl
type publisherImpl struct {
	q       queue
	stop    chan struct{}
	stopped chan struct{}
}

func (impl *publisherImpl) publish(topic string, msg *mq.Message) error {
	if err := kafka.Publish(topic, msg); err == nil {
		return nil
	}

	return impl.q.push(queueName, &message{
		Topic: topic,
		Msg:   *msg,
	})
}

func (impl *publisherImpl) start() {
	go impl.watch()
}

func (impl *publisherImpl) exit() {
	close(impl.stop)

	<-impl.stopped
}

func (impl *publisherImpl) watch() {
	var timer *time.Timer

	defer func() {
		if timer != nil {
			timer.Stop()
		}

		close(impl.stopped)
	}()

	tenMillisecond := 10 * time.Millisecond

	for {
		interval := time.Minute

		if msg, err := impl.q.pop(queueName); err != nil {
			if !impl.q.isEmpty(err) {
				logrus.Error("failed to pop message, err: %s", err.Error())

				interval = tenMillisecond
			}
		} else {
			interval = tenMillisecond

			if err := impl.publish(msg.Topic, &msg.Msg); err != nil {
				logrus.Error("faield to publish message, err:%s", err.Error())
			}
		}

		// time starts.
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-impl.stop:
			return

		case <-timer.C:
		}
	}
}

// message
type message struct {
	Topic string     `json:"topic"`
	Msg   mq.Message `json:"msg"`
}

func (do *message) MarshalBinary() ([]byte, error) {
	return json.Marshal(do)
}

func (do *message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, do)
}
