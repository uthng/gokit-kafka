package kafka

import (
	"context"
	//"encoding/json"
	//"fmt"

	"github.com/go-kit/kit/endpoint"
	//"github.com/go-kit/kit/log"
	//"github.com/go-kit/kit/transport"

	"github.com/Shopify/sarama"
)

// ConsumerGroupHandler represents interface of a consumer group.
type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()

	Start()
	Close() error
}

// ConsumerSessionMessage represents messages to consume.
type ConsumerSessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

// DecodeRequestFunc decodes mesages received by the consumer
type DecodeRequestFunc func(context.Context, interface{}) (request interface{}, err error)

// EncodeResponseFunc encodes response message to publish to "response" topics
type EncodeResponseFunc func(context.Context, interface{}) ([]byte, error)

// ProduceResponseFunc sends response message to a output topic after EncodeResponseFunc invoked
type ProduceResponseFunc func(context.Context, interface{}, *AsyncProducer) error

//type ConsumeRequestFunc func(context.Context, *ConsumerSessionMessage) (interface{}, error)

// ConsumerHandler contains functions to handle encode/decode request/response messages
// and send response mesasges to output topics
type ConsumerHandler struct {
	Endpoint endpoint.Endpoint

	Decode DecodeRequestFunc
	Encode EncodeResponseFunc

	//Consume ConsumeRequestFunc
	Produce ProduceResponseFunc
}

// Handlers is a map between topic and specific handlers
type ConsumerHandlers map[string]ConsumerHandler

// NewConsumerGroup creates a new consumer group and returns a consumer group handler
func NewConsumerGroup(ctx context.Context, brokers []string, topics []string, kind, groupID string, producer *AsyncProducer, handlers ConsumerHandlers) (ConsumerGroupHandler, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_2_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}

	if kind == "multi-async" {
		cg := NewMultiAsyncCG(ctx, client, topics, groupID, 1, 10, producer, handlers)
		return cg, nil
	}

	return nil, err
}
