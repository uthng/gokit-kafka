package kafka

import (
	//"encoding/json"
	//"fmt"
	//"time"
	"context"
	"errors"

	"github.com/Shopify/sarama"

	log "github.com/uthng/golog"
)

var (
	ErrProducerHandlerNotFound = errors.New("producer handler not found")
)

type AsyncProducer struct {
	p sarama.AsyncProducer

	ctx      context.Context
	handlers ProducerHandlers
}

// EncodeRequestFunc decodes mesages received by the consumer
type EncodeRequestFunc func(context.Context, interface{}) ([]byte, error)

// EncodeResponseFunc encodes response message to publish to "response" topics
//type DecodeResponseFunc func(context.Context, []byte, interface{}) error

//type ProduceResponseFunc func(context.Context, interface{}, *AsyncProducer) error

//type ConsumeRequestFunc func(context.Context, *ConsumerSessionMessage) (interface{}, error)

type ProducerHandler struct {
	Encode EncodeRequestFunc
}

type ProducerHandlers map[string]ProducerHandler

// NewProducer creates an instance sarama async producer
func NewAsyncProducer(ctx context.Context, brokers []string, handlers ProducerHandlers) (*AsyncProducer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	return &AsyncProducer{
		p: producer,

		ctx:      ctx,
		handlers: handlers,
	}, nil
}

// Produce sends message to a specified topic
func (p *AsyncProducer) Produce(request interface{}, topic string) error {
	var err error

	handler, ok := p.handlers[topic]
	if !ok {
		log.Errorw("producer handler not found", "topic", topic)
		return ErrProducerHandlerNotFound
	}

	var msg []byte
	if handler.Encode != nil {
		msg, err = handler.Encode(p.ctx, request)
		if err != nil {
			return err
		}
	} else {
		msg = request.([]byte)
	}

	select {
	case p.p.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}:
		log.Infow("Kafka producer sending message...", "type", "async", "topic", topic, "msg", msg)
	case err := <-p.p.Errors():
		log.Errorw("Kafka producer failed to send message", "type", "async", "topic", topic, "msg", msg, "err", err)
		return err
	}

	return nil
}

// Close teminates the instance
func (p *AsyncProducer) Close() error {
	if p != nil {
		return p.p.Close()
	}

	return nil
}
