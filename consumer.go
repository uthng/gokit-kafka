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

	Before    []BeforeFunc
	After     []AfterFunc
	Finalizer []FinalizerFunc

	//Consume ConsumeRequestFunc
	Produce ProduceResponseFunc
}

// ConsumerMsgHandlers is a map between topic and specific handlers
type ConsumerMsgHandlers map[string]*ConsumerMsgHandler

// ConsumerMsgOption set an option param func for ConsumerMsgHandler
type ConsumerMsgOption func(*ConsumerMsgHandler)

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

// NewConsumerMsgHandler creates a new consumer message handler
func NewConsumerMsgHandler(e endpoint.Endpoint, dec DecodeRequestFunc, enc EncodeResponseFunc, p ProduceResponseFunc, options ...ConsumerMsgOption) *ConsumerMsgHandler {
	h := &ConsumerMsgHandler{
		Endpoint: e,
		Decode:   dec,
		Encode:   enc,
		Produce:  p,
	}

	for _, option := range options {
		option(h)
	}

	return h
}

// ConsumerMsgHandlerBefore functions are executed on the publisher request object before the
// request is decoded.
func ConsumerMsgHandlerBefore(before ...BeforeFunc) ConsumerMsgOption {
	return func(h *ConsumerMsgHandler) { h.Before = append(h.Before, before...) }
}

// ConsumerMsgHandlerAfter functions are executed on the consumer reply after the
// endpoint is invoked, but before anything is published to the reply.
func ConsumerMsgHandlerAfter(after ...AfterFunc) ConsumerMsgOption {
	return func(h *ConsumerMsgHandler) { h.After = append(h.After, after...) }
}

// ConsumerMsgHandlerFinalizer functions are executed on the consumer on quitting the function (defer)
func ConsumerMsgHandlerFinalizer(finalizer ...FinalizerFunc) ConsumerMsgOption {
	return func(h *ConsumerMsgHandler) { h.Finalizer = append(h.Finalizer, finalizer...) }
}
