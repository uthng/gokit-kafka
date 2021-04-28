package kafka_test

import (
	//"fmt"
	"context"
	//"os"
	//"os/signal"
	//"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	//"github.com/golang/protobuf/jsonpb"
	"github.com/go-kit/kit/endpoint"
	"github.com/golang/protobuf/proto"

	log "github.com/uthng/golog"

	"github.com/plezi/proxy-provisioner/libs/transport/kafka"

	pb "github.com/plezi/proxy-provisioner/proto/genpb/proxy-provisioner"
	//service "github.com/plezi/proxy-provisioner/service"
)

//func consumeRequest(ctx context.Context, msg *kafka.ConsumerSessionMessage) (interface{}, error) {
//log.Infoln("message consumed:", string(msg.Message.Value))

//return msg.Message.Value, nil
//}

func produceResponse(ctx context.Context, data interface{}, producer *kafka.AsyncProducer) error {
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
	brokers := []string{"localhost:9092"}
	topics := []string{"plezi.tenant.proxy.changed"}
	kind := "multi-async"
	groupID := "proxy-provisioner"

	pbReq := &pb.CreateProxyRequest{
		TenantId: "tenant1",
		Proxy: &pb.Proxy{
			Origin: "analytic.plezi.co",
			Type:   1,
			Proxy:  "app.plezi.co",
		},
	}

	producerHandlers := map[string]kafka.ProducerHandler{
		"plezi.tenant.proxy.changed": kafka.ProducerHandler{
			Encode: encodeRequest,
		},
		"proxy-provisioner.tenant.proxy.changed": kafka.ProducerHandler{
			Encode: nil,
		},
	}

	producer, err := kafka.NewAsyncProducer(context.Background(), brokers, producerHandlers)
	require.Nil(t, err)

	err = producer.Produce(pbReq, topics[0])
	require.Nil(t, err)

	handlers := map[string]kafka.ConsumerHandler{
		"plezi.tenant.proxy.changed": kafka.ConsumerHandler{
			Endpoint: makeTestKafkaEndpoint(),
			Decode:   decodeRequest,
			Encode:   encodeResponse,
			//Consume: consumeRequest,
			Produce: produceResponse,
		},
	}

	cg, err := kafka.NewConsumerGroup(context.Background(), brokers, topics, kind, groupID, producer, handlers)
	require.Nil(t, err)

	cg.Start()

	time.Sleep(5 * time.Second)
	producer.Close()
	cg.Close()
	//require.Nil(t, err)
}
