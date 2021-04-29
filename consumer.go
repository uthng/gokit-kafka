package kafka

import (
	"context"
	//"encoding/json"
	//"fmt"
	//"errors"

	//"github.com/spf13/cast"

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
type ProduceResponseFunc func(context.Context, interface{}, ProducerHandler) error

//type ConsumeRequestFunc func(context.Context, *ConsumerSessionMessage) (interface{}, error)

// ConsumerMsgHandler contains functions to handle encode/decode request/response messages
// and send response mesasges to output topics
type ConsumerMsgHandler struct {
	Endpoint endpoint.Endpoint

	Decode DecodeRequestFunc
	Encode EncodeResponseFunc

	//Consume ConsumeRequestFunc
	Produce ProduceResponseFunc
}

// Handlers is a map between topic and specific handlers
type ConsumerMsgHandlers map[string]ConsumerMsgHandler

// NewConsumerGroup creates a new consumer group and returns a consumer group handler
func NewConsumerGroup(ctx context.Context, client sarama.Client, config map[string]interface{}, producer ProducerHandler, handlers ConsumerMsgHandlers) (ConsumerGroupHandler, error) {
	kind := "multi-async"

	m, ok := config["kind"]
	if ok {
		kind = m.(string)
	}

	if kind == "multi-async" {
		return NewMultiAsyncCG(ctx, client, config, producer, handlers)
	}

	return nil, nil
}
