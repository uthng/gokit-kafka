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
	ErrProducerMsgHandlerNotFound = errors.New("producer handler not found")
)

type ProducerHandler interface {
	Produce(msg interface{}, topic string) error
	Close() error
}

type asyncProducer struct {
	p sarama.AsyncProducer

	ctx      context.Context
	handlers ProducerMsgHandlers
}

// EncodeRequestFunc decodes mesages received by the consumer
type EncodeRequestFunc func(context.Context, interface{}) ([]byte, error)

// EncodeResponseFunc encodes response message to publish to "response" topics
//type DecodeResponseFunc func(context.Context, []byte, interface{}) error

//type ProduceResponseFunc func(context.Context, interface{}, *AsyncProducer) error

//type ConsumeRequestFunc func(context.Context, *ConsumerSessionMessage) (interface{}, error)

type ProducerMsgHandler struct {
	Encode EncodeRequestFunc
}

type ProducerMsgHandlers map[string]ProducerMsgHandler

// NewProducer creates an instance sarama async producer
func NewAsyncProducer(ctx context.Context, client sarama.Client, handlers ProducerMsgHandlers) (ProducerHandler, error) {
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &asyncProducer{
		p: producer,

		ctx:      ctx,
		handlers: handlers,
	}, nil
}

// Produce sends message to a specified topic
func (p *asyncProducer) Produce(request interface{}, topic string) error {
	var err error

	handler, ok := p.handlers[topic]
	if !ok {
		log.Errorw("producer handler not found", "topic", topic)
		return ErrProducerMsgHandlerNotFound
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
func (p *asyncProducer) Close() error {
	if p != nil {
		return p.p.Close()
	}

	return nil
}
