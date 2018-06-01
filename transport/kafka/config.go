package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/micro/go-micro/transport"
)

var kafkaVersion = sarama.V1_0_0_0

func newDefaultOptions() transport.Options {
	return transport.Options{
		Addrs: []string{"localhost:9092"},
	}
}

func newSaramaConfig(opts transport.Options) *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = kafkaVersion
	saramaConfig.Net.TLS.Enable = opts.Secure
	saramaConfig.Net.TLS.Config = opts.TLSConfig
	return saramaConfig
}

func newClusterConfig(opts transport.Options) *cluster.Config {
	clusterConfig := cluster.NewConfig()
	clusterConfig.Version = kafkaVersion
	clusterConfig.Net.TLS.Enable = opts.Secure
	clusterConfig.Net.TLS.Config = opts.TLSConfig
	return clusterConfig
}
