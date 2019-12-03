package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"go.uber.org/zap"
)

type KafkaConfig struct {
	Servers      []string `json:"servers"`
	UserName     string   `json:"username"`
	Password     string   `json:"password"`
	CertFile     string   `json:"cert_file"`
	Retry        int      `json:"retry"`
	Concurrent   int      `json:"concurrent"`
	IsProduction bool     `json:"is_production"`
	Topic        string   `json:"topic"`
	ConsumerId   string   `json:"consumer_id"`
}

var (
	kafkaConf KafkaConfig
)

func InitKafka(conf KafkaConfig) {
	kafkaConf = conf
	/*if conf.CertFile != "" {
		_, err := ioutil.ReadFile(conf.CertFile)
		if err != nil {
			panic(fmt.Sprintf("read MQ cert file error: %v", err))
		}
	}*/

	var (
		errStr = "kafka %s configuration empty"
	)

	if len(kafkaConf.Servers) == 0 {
		panic(fmt.Sprintf(errStr, "servers"))
	}
}

func GetKafkaConfig() KafkaConfig {
	return kafkaConf
}

//---------------

type KafkaClient struct {
}

var (
	GKafkaCliList = make([]sarama.AsyncProducer, 0)
)

func NewKafkaProducer(servers []string, logger *zap.Logger) (sarama.AsyncProducer, error) {
	if len(GKafkaCliList) != 0 {
		return GKafkaCliList[0], nil
	}

	mqConfig, err := NewKafkaProducerConfig()
	if mqConfig == nil || err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(servers, mqConfig)
	if err != nil {
		return nil, err
	}

	GKafkaCliList = append(GKafkaCliList, producer)
	go checkProducer(producer, logger)

	return producer, nil
}

func checkProducer(p sarama.AsyncProducer, logger *zap.Logger) {
	for {
		select {
		case msg := <-p.Successes():
			logger.Debug("success push kafka message",
				zap.Any("data", msg))
		case err := <-p.Errors():
			logger.Error("write in queue error",
				zap.Error(err))
		}
	}
}

func NewKafkaConsumer(consumerId string, topics []string, servers []string) (*cluster.Consumer, error) {
	mqConfig, err := NewKafkaConsumerConfig()
	if mqConfig == nil || err != nil {
		return nil, err
	}

	consumer, err := cluster.NewConsumer(servers, consumerId, topics, mqConfig)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func NewKafkaProducerConfig() (*sarama.Config, error) {
	mqConfig := sarama.NewConfig()

	if kafkaConf.UserName != "" && kafkaConf.Password != "" {
		if kafkaConf.IsProduction {
			mqConfig.Net.SASL.Enable = false
		} else {
			mqConfig.Net.SASL.Enable = true
		}
		mqConfig.Net.SASL.User = kafkaConf.UserName
		mqConfig.Net.SASL.Password = kafkaConf.Password
		mqConfig.Net.SASL.Handshake = true
	}

	if kafkaConf.CertFile != "" {
		certBytes, err := ioutil.ReadFile(kafkaConf.CertFile)
		if err != nil {
			return nil, err
		}

		clientCertPool := x509.NewCertPool()
		ok := clientCertPool.AppendCertsFromPEM(certBytes)
		if !ok {
			return nil, errors.New("cert file empty")
		}

		mqConfig.Net.TLS.Config = &tls.Config{
			RootCAs:            clientCertPool,
			InsecureSkipVerify: true,
		}
		if kafkaConf.IsProduction {
			mqConfig.Net.TLS.Enable = false
		} else {
			mqConfig.Net.TLS.Enable = true
		}
	}
	mqConfig.Producer.Return.Successes = true

	if err := mqConfig.Validate(); err != nil {
		return nil, err
	}

	return mqConfig, nil
}

func NewKafkaConsumerConfig() (*cluster.Config, error) {
	clusterCfg := cluster.NewConfig()

	if kafkaConf.UserName != "" && kafkaConf.Password != "" {
		if kafkaConf.IsProduction {
			clusterCfg.Net.SASL.Enable = false
		} else {
			clusterCfg.Net.SASL.Enable = true
		}
		clusterCfg.Net.SASL.User = kafkaConf.UserName
		clusterCfg.Net.SASL.Password = kafkaConf.Password
		clusterCfg.Net.SASL.Handshake = true
	}

	if kafkaConf.CertFile != "" {
		certBytes, err := ioutil.ReadFile(kafkaConf.CertFile)
		if err != nil {
			return nil, err
		}
		clientCertPool := x509.NewCertPool()
		ok := clientCertPool.AppendCertsFromPEM(certBytes)
		if !ok {
			return nil, errors.New("read cert file error")
		}

		clusterCfg.Net.TLS.Config = &tls.Config{
			RootCAs:            clientCertPool,
			InsecureSkipVerify: true,
		}

		if kafkaConf.IsProduction {
			clusterCfg.Net.TLS.Enable = false
		} else {
			clusterCfg.Net.TLS.Enable = true
		}
	}

	clusterCfg.Consumer.Return.Errors = true
	clusterCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	clusterCfg.Group.Return.Notifications = true

	clusterCfg.Version = sarama.V0_10_0_0
	if err := clusterCfg.Validate(); err != nil {
		return nil, err
	}

	return clusterCfg, nil
}
