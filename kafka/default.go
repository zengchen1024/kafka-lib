package kafka

import "github.com/opensourceways/kafka-lib/mq"

var (
	defaultMQ = NewMQ()
)

func Init(opts ...mq.Option) error {
	return defaultMQ.Init(opts...)
}

func Connect() error {
	return defaultMQ.Connect()
}

func Disconnect() error {
	return defaultMQ.Disconnect()
}

func Publish(topic string, msg *mq.Message, opts ...mq.PublishOption) error {
	return defaultMQ.Publish(topic, msg, opts...)
}

/*
func Subscribe(group string, handler mq.Handler, topics []string) (mq.Subscriber, error) {
	return defaultMQ.Subscribe(group, handler, topics)
}
*/

func Subscribe(h mq.Handler, topics []string, opts ...mq.SubscribeOption) (mq.Subscriber, error) {
	return defaultMQ.Subscribe(h, topics, opts...)
}

func String() string {
	return defaultMQ.String()
}
