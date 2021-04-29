package kafka

import (
	"context"
	//"encoding/json"
	//"fmt"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"
)

var (
	ErrKafkaConfigMissing         = errors.New("kafka config is missing")
	ErrKafkaBrokersMissing        = errors.New("kafka brokers is missing")
	ErrKafkaConsumerConfigMissing = errors.New("kafka consumer config is missing")
)

type Listener struct {
	P  ProducerHandler
	CG ConsumerGroupHandler
}

func NewListener(ctx context.Context, config map[string]interface{}, pMsgHandlers ProducerMsgHandlers, cMsgHandlers ConsumerMsgHandlers) (*Listener, error) {
	brokers := []string{}
	offset := sarama.OffsetNewest

	m, ok := config["brokers"]
	if !ok {
		return nil, ErrKafkaBrokersMissing
	}

	brokers = cast.ToStringSlice(m)

	m, ok = config["config"]
	if !ok {
		return nil, ErrKafkaConfigMissing
	}

	cfgCommon := cast.ToStringMap(m)

	m, ok = cfgCommon["offset"]
	if ok {
		offset = cast.ToInt64(m)
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_7_0_0
	cfg.Consumer.Offsets.Initial = offset

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, err
	}

	pHandler, err := NewAsyncProducer(ctx, client, pMsgHandlers)
	if err != nil {
		return nil, err
	}

	m, ok = config["consumer"]
	if !ok {
		return nil, ErrKafkaConsumerConfigMissing
	}
	cfgConsumer := cast.ToStringMap(m)

	cgHandler, err := NewConsumerGroup(ctx, client, cfgConsumer, pHandler, cMsgHandlers)
	if err != nil {
		return nil, err
	}

	return &Listener{
		P:  pHandler,
		CG: cgHandler,
	}, nil
}

func (l *Listener) Listen() {
	l.CG.Start()
}

func (l *Listener) Close() {
	l.P.Close()
	l.CG.Close()
}
