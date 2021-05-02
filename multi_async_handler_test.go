package kafka_test

import (
	//"fmt"
	"context"
	//"os"
	//"os/signal"
	//"syscall"
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	//"github.com/golang/protobuf/jsonpb"
	"github.com/go-kit/kit/endpoint"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"

	"github.com/Shopify/sarama"

	log "github.com/uthng/golog"

	"github.com/plezi/proxy-provisioner/libs/transport/kafka"

	pb "github.com/plezi/proxy-provisioner/proto/genpb/proxy-provisioner"
	//service "github.com/plezi/proxy-provisioner/service"
)

type contextKey string

var (
	TopicPleziTenantProxyChanged            = "plezi.tenant.proxy.changed"
	TopicProxyProvisionerTenantProxyChanged = "proxy-provisioner.tenant.proxy.changed"

	ContextKeyTestingT = contextKey("testing.T")
)

func getContextKeyValue(ctx context.Context, k contextKey) interface{} {
	if v := ctx.Value(k); v != nil {
		return v
	}

	return nil
}

func produceResponse(ctx context.Context, data interface{}, producer kafka.ProducerHandler) error {
	err := producer.Produce(data, TopicProxyProvisionerTenantProxyChanged)

	return err
}

func decodeRequest(ctx context.Context, request interface{}) (interface{}, error) {
	req := request.(*kafka.ConsumerSessionMessage)

	pbReq := &pb.HandleProxyRequest{
		Proxy: &pb.Proxy{},
	}

	err := proto.Unmarshal(req.Message.Value, pbReq)
	if err != nil {
		return nil, err
	}

	return pbReq, nil
}

func encodeResponse(ctx context.Context, response interface{}) ([]byte, error) {
	msgBytes, err := proto.Marshal(response.(*pb.HandleProxyRequest))
	if err != nil {
		return nil, err
	}

	return msgBytes, nil
}

func encodeRequest(ctx context.Context, request interface{}) ([]byte, error) {
	msgBytes, err := proto.Marshal(request.(*pb.HandleProxyRequest))
	if err != nil {
		return nil, err
	}

	return msgBytes, nil
}

func makeTransformKafkaEndpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(*pb.HandleProxyRequest)

		req.TenantId = "output"
		req.Proxy.Origin = "output.kafka.topic"

		return request, nil
	}
}

func initTestEnv(brokers []string) error {
	topics := []string{TopicPleziTenantProxyChanged, TopicProxyProvisionerTenantProxyChanged}

	log.Infow("Initializing cluster kafka for test...")
	clusterAdmin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		return err
	}

	defer clusterAdmin.Close()

	clusterTopics, err := clusterAdmin.ListTopics()
	if err != nil {
		return err
	}

	for _, name := range topics {
		if _, ok := clusterTopics[name]; ok {
			log.Infow("Deleting topic...", "topic", name)
			err := clusterAdmin.DeleteTopic(name)
			if err != nil {
				log.Errorln(err)
				return err
			}

			time.Sleep(5 * time.Second)
		}
	}

	for _, name := range topics {
		log.Infow("Creating topic...", "topic", name)
		err := clusterAdmin.CreateTopic(name, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
		if err != nil {
			log.Errorln(err)
			return err
		}

		time.Sleep(5 * time.Second)
	}

	return nil
}

// TestConsumer tests the message flow from producer to consumer with different steps.
// In this test, the producer and the consumer, each one plays 2 roles: internal et external.
// - Producer as external client produces a message to the topic for which is waiting the consumer
// - Consumer reads the message, applies beforefunc, decodes, calls service via endpoint, applies afterfunc, encodes and write it to the output topic
// - Consumer as external client reads the output topic
func TestConsumer(t *testing.T) {

	pbReq := &pb.HandleProxyRequest{
		TenantId: "tenant1",
		Action:   pb.ProxyAction_CREATE,
		Proxy: &pb.Proxy{
			Origin:   "analytic.plezi.co",
			Type:     1,
			Proxy:    "app.plezi.co",
			Verified: false,
		},
	}

	kafkaConfig := []byte(`
kafka:
  brokers:
    - localhost:9092
  config:
    offsetInitial: newest
  consumer:
    kind: multi-async
    groupID: proxy-provisioner
    nbWorkers: 1
    bufferSize: 10
    topics:
      - ` + TopicPleziTenantProxyChanged + `
      - ` + TopicProxyProvisionerTenantProxyChanged + `
  producer:
    topics:
      - name: proxy-provisioner.tenant.proxy.changed
        event: tenantProxyChanged
`)

	viper.SetConfigType("yaml")
	viper.ReadConfig(bytes.NewBuffer(kafkaConfig))

	cfg := viper.GetStringMap("kafka")
	ctx := context.Background()

	ctx = context.WithValue(ctx, ContextKeyTestingT, t)

	err := initTestEnv(viper.GetStringSlice("kafka.brokers"))
	require.Nil(t, err)

	pMsgHandlers := map[string]*kafka.ProducerMsgHandler{
		TopicPleziTenantProxyChanged: kafka.NewProducerMsgHandler(
			encodeRequest,
			[]kafka.ProducerMsgOption{}...),
		TopicProxyProvisionerTenantProxyChanged: kafka.NewProducerMsgHandler(
			nil,
			[]kafka.ProducerMsgOption{}...),
	}

	cMsgHandlers := map[string]*kafka.ConsumerMsgHandler{
		TopicPleziTenantProxyChanged: kafka.NewConsumerMsgHandler(
			makeTransformKafkaEndpoint(),
			decodeRequest,
			encodeResponse,
			produceResponse,
			[]kafka.ConsumerMsgOption{}...),
		TopicProxyProvisionerTenantProxyChanged: kafka.NewConsumerMsgHandler(
			nil,
			decodeRequest,
			nil,
			nil,
			kafka.ConsumerMsgHandlerAfter(func(ctx context.Context, data interface{}) context.Context {
				expected := &pb.HandleProxyRequest{
					TenantId: "output",
					Action:   pb.ProxyAction_CREATE,
					Proxy: &pb.Proxy{
						Origin:   "output.kafka.topic",
						Type:     pb.ProxyType_FIRST_PARTY_COOKIE,
						Proxy:    "app.plezi.co",
						Verified: false,
					},
				}

				result := data.(*pb.HandleProxyRequest)

				t := getContextKeyValue(ctx, ContextKeyTestingT).(*testing.T)

				byte1, err := proto.Marshal(expected)
				require.Nil(t, err)

				byte2, err := proto.Marshal(result)
				require.Nil(t, err)

				log.Infow("Comparing test values...")
				require.Equal(t, byte1, byte2)

				return ctx
			})),
	}

	kafkaListener, err := kafka.NewListener(ctx, cfg, pMsgHandlers, cMsgHandlers)
	require.Nil(t, err)

	kafkaListener.Listen()
	kafkaListener.P.Produce(pbReq, TopicPleziTenantProxyChanged)

	time.Sleep(5 * time.Second)

	kafkaListener.Close()
}
