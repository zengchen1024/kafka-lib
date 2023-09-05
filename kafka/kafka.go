package kafka

import (
	"context"
	"errors"
	"sync"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/opensourceways/kafka-lib/mq"
)

type kfkMQ struct {
	opts     mq.Options
	producer sarama.SyncProducer

	mutex     sync.RWMutex
	connected bool
}

func (impl *kfkMQ) Init(opts ...mq.Option) error {
	if impl.isConnected() {
		return errors.New("mq is connected and can't do init")
	}

	impl.mutex.Lock()
	defer impl.mutex.Unlock()

	if impl.connected {
		return nil
	}

	for _, o := range opts {
		o(&impl.opts)
	}

	if impl.opts.Addresses == nil {
		impl.opts.Addresses = []string{"127.0.0.1:9092"}
	}

	if impl.opts.Context == nil {
		impl.opts.Context = context.Background()
	}

	if impl.opts.Codec == nil {
		impl.opts.Codec = mq.JsonCodec{}
	}

	return nil
}

func (impl *kfkMQ) isConnected() (b bool) {
	impl.mutex.RLock()
	b = impl.connected
	impl.mutex.RUnlock()

	return
}

func (impl *kfkMQ) Options() mq.Options {
	return impl.opts
}

func (impl *kfkMQ) Address() string {
	if len(impl.opts.Addresses) > 0 {
		return impl.opts.Addresses[0]
	}

	return ""
}

func (impl *kfkMQ) Connect() error {
	if impl.isConnected() {
		return nil
	}

	impl.mutex.Lock()
	defer impl.mutex.Unlock()

	if impl.connected {
		return nil
	}

	producer, err := sarama.NewSyncProducer(impl.opts.Addresses, impl.clusterConfig())
	if err != nil {
		return err
	}

	impl.producer = producer
	impl.connected = true

	return nil
}

func (impl *kfkMQ) Disconnect() error {
	if !impl.isConnected() {
		return nil
	}

	impl.mutex.Lock()
	defer impl.mutex.Unlock()

	if !impl.connected {
		return nil
	}

	impl.connected = false

	return impl.producer.Close()
}

// Publish a message to a topic in the kafka cluster.
func (impl *kfkMQ) Publish(topic string, msg *mq.Message, opts ...mq.PublishOption) error {
	d, err := impl.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}

	pm := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(d),
	}

	if key := msg.MessageKey(); key != "" {
		pm.Key = sarama.StringEncoder(key)
	}

	_, _, err = impl.producer.SendMessage(pm)

	return err
}

// Subscribe to kafka message topic, each subscription generates a kafka groupConsumer group.
func (impl *kfkMQ) Subscribe(h mq.Handler, topics []string, opts ...mq.SubscribeOption) (mq.Subscriber, error) {
	opt := mq.SubscribeOptions{
		AutoAck: true,
		Queue:   uuid.New().String(),
	}
	for _, o := range opts {
		o(&opt)
	}
	if opt.Context == nil {
		opt.Context = context.Background()
	}

	c, err := impl.saramaClusterClient()
	if err != nil {
		return nil, err
	}

	g, err := sarama.NewConsumerGroupFromClient(opt.Queue, c)
	if err != nil {
		c.Close()

		return nil, err
	}

	s := newSubscriber(topics, c, g, &impl.opts, &opt)

	if err = s.start(impl.genHanler(h, &opt)); err != nil {
		g.Close()
		c.Close()
	}

	return s, err
}

func (impl *kfkMQ) String() string {
	return "kafka"
}

func (impl *kfkMQ) clusterConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3

	if impl.opts.TLSConfig != nil {
		cfg.Net.TLS.Config = impl.opts.TLSConfig
		cfg.Net.TLS.Enable = true
	}

	cfg.Version = impl.opts.Version

	// no need to handle error
	// cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	return cfg
}

func (impl *kfkMQ) saramaClusterClient() (sarama.Client, error) {
	return sarama.NewClient(impl.opts.Addresses, impl.clusterConfig())
}

func NewMQ(opts ...mq.Option) mq.MQ {
	options := mq.Options{
		Codec:   mq.JsonCodec{},
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	if len(options.Addresses) == 0 {
		options.Addresses = []string{"127.0.0.1:9092"}
	}

	if options.Log == nil {
		options.Log = mq.NewLogger()
	}

	return &kfkMQ{
		opts: options,
	}
}
