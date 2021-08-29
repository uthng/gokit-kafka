package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	//"encoding/json"
	//"fmt"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"

	log "github.com/uthng/golog"
)

var (
	ErrKafkaConfigMissing         = errors.New("kafka config is missing")
	ErrKafkaBrokersMissing        = errors.New("kafka brokers is missing")
	ErrKafkaConsumerConfigMissing = errors.New("kafka consumer config is missing")
	ErrKafkaTLSCACertMissing      = errors.New("kafka TLS CA certificate is missing")
	ErrKafkaTLSUserCertMissing    = errors.New("kafka TLS User certificate is missing")
	ErrKafkaTLSUserKeyMissing     = errors.New("kafka TLS User key is missing")
	ErrKafkaTLSConfig             = errors.New("failed to create TLS Config")
)

type Listener struct {
	brokers []string

	C  sarama.Client
	P  ProducerHandler
	CG ConsumerGroupHandler
}

func NewListener(ctx context.Context, config map[string]interface{}, pMsgHandlers ProducerMsgHandlers, cMsgHandlers ConsumerMsgHandlers) (*Listener, error) {
	var tlsConfig *tls.Config

	brokers := []string{}
	offset := sarama.OffsetNewest
	caCert := ""
	userCert := ""
	userKey := ""
	insecureSkipVerify := false

	m, ok := config["brokers"]
	if !ok {
		return nil, ErrKafkaBrokersMissing
	}

	brokers = cast.ToStringSlice(m)

	// Parse TLS
	m, ok = config["tls"]
	if ok {
		tls := cast.ToStringMap(m)

		log.Debugln("Configuration TLS", "tls", tls)

		m, ok = tls["usercert"]
		if !ok {
			return nil, ErrKafkaTLSUserCertMissing
		}

		userCert = cast.ToString(m)

		m, ok = tls["userkey"]
		if !ok {
			return nil, ErrKafkaTLSUserKeyMissing
		}

		userKey = cast.ToString(m)

		m, ok = tls["cacert"]
		if ok {
			caCert = cast.ToString(m)
		}

		m, ok = tls["insecureskipverify"]
		if ok {
			insecureSkipVerify = cast.ToBool(m)
		}

		var err error
		tlsConfig, err = newTLSConfig(caCert, userCert, userKey)
		if err != nil {
			return nil, ErrKafkaTLSConfig
		}

		tlsConfig.InsecureSkipVerify = insecureSkipVerify
	}

	m, ok = config["config"]
	if !ok {
		return nil, ErrKafkaConfigMissing
	}

	cfgCommon := cast.ToStringMap(m)

	m, ok = cfgCommon["offset"]
	if ok {
		offset = cast.ToInt64(m)
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Offsets.Initial = offset

	if tlsConfig != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsConfig
	}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, err
	}

	pHandler, err := NewAsyncProducer(ctx, client, pMsgHandlers)
	if err != nil {
		return nil, err
	}

	m, ok = config["consumer"]
	if !ok {
		return nil, ErrKafkaConsumerConfigMissing
	}
	cfgConsumer := cast.ToStringMap(m)

	cgHandler, err := NewConsumerGroup(ctx, client, cfgConsumer, pHandler, cMsgHandlers)
	if err != nil {
		return nil, err
	}

	return &Listener{
		brokers: brokers,
		C:       client,
		P:       pHandler,
		CG:      cgHandler,
	}, nil
}

func (l *Listener) Listen() {
	log.Infow("Kafka Listener starts...", "brokers", l.brokers)
	l.CG.Start()
}

func (l *Listener) Close() {
	l.P.Close()
	l.CG.Close()
}

///////////// INTERNAL FUNC ///////////////

func newTLSConfig(caCert, userCert, userKey string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(userCert, userKey)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	if caCert != "" {
		cacert, err := ioutil.ReadFile(caCert)
		if err != nil {
			return &tlsConfig, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cacert)
		tlsConfig.RootCAs = caCertPool
	}

	//tlsConfig.BuildNameToCertificate()

	return &tlsConfig, err
}
