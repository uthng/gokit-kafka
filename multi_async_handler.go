package kafka

import (
	"context"

	"github.com/Shopify/sarama"

	log "github.com/uthng/golog"
)

type MultiAsyncCG struct {
	context context.Context

	CG sarama.ConsumerGroup

	Topics  []string
	GroupID string

	NbWorkers  int
	BufferSize int

	producer *AsyncProducer
	handlers ConsumerHandlers

	CfgHandler MultiAsyncCGHandlerConfig
}

type MultiAsyncCGHandlerConfig struct {
	BufChan chan *ConsumerSessionMessage

	ready chan bool
}

func NewMultiAsyncCG(ctx context.Context, cg sarama.ConsumerGroup, topics []string, groupID string, nbWorkers, bufferSize int, producer *AsyncProducer, handlers ConsumerHandlers) *MultiAsyncCG {
	macg := &MultiAsyncCG{
		context:    ctx,
		CG:         cg,
		Topics:     topics,
		GroupID:    groupID,
		NbWorkers:  nbWorkers,
		BufferSize: bufferSize,
		producer:   producer,
		handlers:   handlers,
	}

	cfgHandler := MultiAsyncCGHandlerConfig{
		BufChan: make(chan *ConsumerSessionMessage, bufferSize),
		ready:   make(chan bool, 0),
	}

	macg.CfgHandler = cfgHandler

	return macg
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (macg *MultiAsyncCG) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(macg.CfgHandler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (macg *MultiAsyncCG) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (macg *MultiAsyncCG) WaitReady() {
	<-macg.CfgHandler.ready
	return
}

func (macg *MultiAsyncCG) Reset() {
	macg.CfgHandler.ready = make(chan bool, 0)
	return
}

func (macg *MultiAsyncCG) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for message := range claimMsgChan {
		macg.CfgHandler.BufChan <- &ConsumerSessionMessage{
			Session: session,
			Message: message,
		}
	}

	return nil
}

func (macg *MultiAsyncCG) Close() error {
	return macg.CG.Close()
}

func (macg *MultiAsyncCG) Start() {
	go func() {
		for {
			err := macg.CG.Consume(macg.context, macg.Topics, macg)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					log.Fatalln(err)
				}
			}
			if macg.context.Err() != nil {
				return
			}

			macg.Reset()
		}
	}()

	macg.WaitReady() // Await till the consumer has been set up

	for i := 0; i < macg.NbWorkers; i++ {
		go func() {
			for message := range macg.CfgHandler.BufChan {
				var err error

				topic := message.Message.Topic

				handler, ok := macg.handlers[topic]
				if !ok {
					log.Errorw("Producer handler not found", "topic", topic, "msg", string(message.Message.Value))
					continue
				}

				var req interface{}
				if handler.Decode != nil {
					req, err = handler.Decode(macg.context, message)
					if err != nil {
						log.Errorw("Failed to handle the message", "topic", topic, "msg", string(message.Message.Value), "err", err)
						continue
					}
				} else {
					req = message.Message.Value
				}

				// Apply endpoint with service
				var resp interface{}
				if handler.Endpoint != nil {
					resp, err = handler.Endpoint(macg.context, req)
					if err != nil {
						log.Errorw("Failed to apply endpoint to request message", "req", req, "err", err)
						continue
					}
				} else {
					resp = req
				}

				// Encode response from service endoint
				var msg []byte
				if handler.Encode != nil {
					msg, err = handler.Encode(macg.context, resp)
					if err != nil {
						log.Errorw("Failed to encode response message", "resp", resp, "err", err)
						continue
					}
				} else {
					msg = resp.([]byte)
				}

				if handler.Produce != nil && macg.producer != nil {
					err = handler.Produce(macg.context, msg, macg.producer)
					if err != nil {
						log.Errorw("Failed to produce response message", "msg", string(msg), "err", err)
						continue
					}

				}

				message.Session.MarkMessage(message.Message, "")
			}
		}()
	}
}
