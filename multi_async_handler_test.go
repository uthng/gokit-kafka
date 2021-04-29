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

	log "github.com/uthng/golog"

	"github.com/plezi/proxy-provisioner/libs/transport/kafka"

	pb "github.com/plezi/proxy-provisioner/proto/genpb/proxy-provisioner"
	//service "github.com/plezi/proxy-provisioner/service"
)

//func consumeRequest(ctx context.Context, msg *kafka.ConsumerSessionMessage) (interface{}, error) {
//log.Infoln("message consumed:", string(msg.Message.Value))

//return msg.Message.Value, nil
//}

func produceResponse(ctx context.Context, data interface{}, producer kafka.ProducerHandler) error {
	err := producer.Produce(data, "proxy-provisioner.tenant.proxy.changed")

	return err
}

func decodeRequest(ctx context.Context, request interface{}) (interface{}, error) {
	req := request.(*kafka.ConsumerSessionMessage)

	pbReq := &pb.CreateProxyRequest{
		Proxy: &pb.Proxy{},
	}

	err := proto.Unmarshal(req.Message.Value, pbReq)
	if err != nil {
		return nil, err
	}

	return pbReq, nil
}

func encodeResponse(ctx context.Context, response interface{}) ([]byte, error) {
	msgBytes, err := proto.Marshal(response.(*pb.CreateProxyRequest))
	if err != nil {
		return nil, err
	}

	return msgBytes, nil
}

func encodeRequest(ctx context.Context, request interface{}) ([]byte, error) {
	msgBytes, err := proto.Marshal(request.(*pb.CreateProxyRequest))
	if err != nil {
		return nil, err
	}

	return msgBytes, nil
}

func makeTestKafkaEndpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		log.Warnln("Hello ! This is an endpoint.")
		log.Warnln("Endpoint request:", request)

		return request, nil
	}
}

func TestConsumer(t *testing.T) {

	//testCases := []struct {
	//name   string
	//req    *pb.CreateProxyRequest
	//err    interface{}
	//result map[string]interface{}
	//}{
	//{
	//},

	//for _, tc := range testCases {
	//t.Run(tc.name, func(t *testing.T) {

	//})
	//}
	//brokers := []string{"localhost:9092"}
	//topics := []string{"plezi.tenant.proxy.changed"}
	//kind := "multi-async"
	//groupID := "proxy-provisioner"

	pbReq := &pb.CreateProxyRequest{
		TenantId: "tenant1",
		Proxy: &pb.Proxy{
			Origin: "analytic.plezi.co",
			Type:   1,
			Proxy:  "app.plezi.co",
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
      - plezi.tenant.proxy.changed
  producer:
    topics:
      - name: proxy-provisioner.tenant.proxy.changed
        event: tenantProxyChanged
`)

	viper.SetConfigType("yaml")
	viper.ReadConfig(bytes.NewBuffer(kafkaConfig))

	cfg := viper.GetStringMap("kafka")

	pMsgHandlers := map[string]kafka.ProducerMsgHandler{
		"plezi.tenant.proxy.changed": kafka.ProducerMsgHandler{
			Encode: encodeRequest,
		},
		"proxy-provisioner.tenant.proxy.changed": kafka.ProducerMsgHandler{
			Encode: nil,
		},
	}

	//producer, err := kafka.NewAsyncProducer(context.Background(), brokers, producerHandlers)
	//require.Nil(t, err)

	//err = producer.Produce(pbReq, topics[0])
	//require.Nil(t, err)

	cMsgHandlers := map[string]kafka.ConsumerMsgHandler{
		"plezi.tenant.proxy.changed": kafka.ConsumerMsgHandler{
			Endpoint: makeTestKafkaEndpoint(),
			Decode:   decodeRequest,
			Encode:   encodeResponse,
			//Consume: consumeRequest,
			Produce: produceResponse,
		},
	}

	kafkaListener, err := kafka.NewListener(context.Background(), cfg, pMsgHandlers, cMsgHandlers)
	require.Nil(t, err)

	//cg, err := kafka.NewConsumerGroup(context.Background(), brokers, topics, kind, groupID, producer, handlers)
	//require.Nil(t, err)

	//cg.Start()

	kafkaListener.P.Produce(pbReq, "plezi.tenant.proxy.changed")
	kafkaListener.Listen()

	time.Sleep(5 * time.Second)

	kafkaListener.Close()

	//producer.Close()
	//cg.Close()
	//require.Nil(t, err)
}
