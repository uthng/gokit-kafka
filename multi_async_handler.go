package kafka

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"

	log "github.com/uthng/golog"
)

var (
	// ErrConsumerTopicsMissing indicates that the consumer topics config is missing
	ErrConsumerTopicsMissing = errors.New("consumer topics is missing")
)

type multiAsyncCG struct {
	context context.Context

	cg sarama.ConsumerGroup

	topics     []string
	groupID    string
	nbWorkers  int
	bufferSize int

	cfg map[string]interface{}

	producer ProducerHandler
	handlers ConsumerMsgHandlers

	cfgHandler multiAsyncCGHandlerConfig
}

type multiAsyncCGHandlerConfig struct {
	bufChan chan *ConsumerSessionMessage

	ready chan bool
}

// NewMultiAsyncCG creates a new multi async consumer group and return consumer group handler
func NewMultiAsyncCG(ctx context.Context, client sarama.Client, cfg map[string]interface{}, producer ProducerHandler, handlers ConsumerMsgHandlers) (ConsumerGroupHandler, error) {
	groupID := "proxy-provisioner"
	topics := []string{}
	nbWorkers := 1
	bufferSize := 10

	m, ok := cfg["groupID"]
	if ok {
		groupID = cast.ToString(m)
	}

	cg, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, err
	}

	m, ok = cfg["topics"]
	if !ok {
		return nil, ErrConsumerTopicsMissing
	}

	sTopics := cast.ToSlice(m)
	for _, k := range sTopics {
		m := cast.ToStringMapString(k)
		topics = append(topics, m["name"])
	}

	m, ok = cfg["nbWorkers"]
	if ok {
		nbWorkers = cast.ToInt(m)
	}

	m, ok = cfg["bufferSize"]
	if ok {
		bufferSize = cast.ToInt(m)
	}

	macg := &multiAsyncCG{
		context:    ctx,
		cg:         cg,
		topics:     topics,
		groupID:    groupID,
		nbWorkers:  nbWorkers,
		bufferSize: bufferSize,
		producer:   producer,
		handlers:   handlers,
	}

	cfgHandler := multiAsyncCGHandlerConfig{
		bufChan: make(chan *ConsumerSessionMessage, bufferSize),
		ready:   make(chan bool, 0),
	}

	macg.cfgHandler = cfgHandler

	return macg, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (macg *multiAsyncCG) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(macg.cfgHandler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (macg *multiAsyncCG) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// WaitReady waits for the consumer ready
func (macg *multiAsyncCG) WaitReady() {
	<-macg.cfgHandler.ready
	return
}

// Reset resets consumer ready
func (macg *multiAsyncCG) Reset() {
	macg.cfgHandler.ready = make(chan bool, 0)
	return
}

// ConsumeClaim claims messages from topics
func (macg *multiAsyncCG) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for message := range claimMsgChan {
		macg.cfgHandler.bufChan <- &ConsumerSessionMessage{
			Session: session,
			Message: message,
		}
	}

	return nil
}

// Close terminates the consumer group
func (macg *multiAsyncCG) Close() error {
	return macg.cg.Close()
}

// Start launches different goroutines: one for consuming messages from topics and others for the workers to handle messages arrving to the buffer channel.
func (macg *multiAsyncCG) Start() {
	go func() {
		log.Infow("Kafka Multi Async Consumer Group starts consuming...", "groupID", macg.groupID, "topics", macg.topics)
		for {
			err := macg.cg.Consume(macg.context, macg.topics, macg)
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

	log.Infow("Kafka Consumer Worker start...", "nbWorkers", macg.nbWorkers, "bufferSize", macg.bufferSize)
	for i := 0; i < macg.nbWorkers; i++ {
		go func() {
			//log.Warnln("bufchan", macg.cfgHandler.bufChan)
			for message := range macg.cfgHandler.bufChan {
				var err error

				topic := message.Message.Topic

				handler, ok := macg.handlers[topic]
				if !ok {
					log.Errorw("Producer handler not found", "topic", topic, "msg", string(message.Message.Value))
					continue
				}

				if len(handler.Finalizer) > 0 {
					defer func() {
						for _, f := range handler.Finalizer {
							f(macg.context, message)
						}
					}()
				}

				log.Infow("Kafka consumer receiving message...", "topic", topic, "msg", message.Message.Value)

				// Execute les BEFORE funcs
				for _, f := range handler.Before {
					macg.context = f(macg.context, message)
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

				// Execute les AFTER funcs
				for _, f := range handler.After {
					macg.context = f(macg.context, resp)
				}

				// Encode response from service endoint
				var msg interface{}
				if handler.Encode != nil {
					msg, err = handler.Encode(macg.context, resp)
					if err != nil {
						log.Errorw("Failed to encode response message", "resp", resp, "err", err)
						continue
					}
				} else {
					msg = resp
				}

				if handler.Produce != nil && macg.producer != nil {
					err = handler.Produce(macg.context, msg, macg.producer)
					if err != nil {
						log.Errorw("Failed to produce response message", "msg", msg.(string), "err", err)
						continue
					}

				}

				message.Session.MarkMessage(message.Message, "")
			}
		}()
	}
}
