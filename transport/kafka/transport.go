// Package kafka is a go-micro transport plugin that provides ability to make RPC
// calls via Apache Kafka.
package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/transport"
	"github.com/nu7hatch/gouuid"
)

const (
	suffSend = "-send"
	suffRecv = "-recv"
)

// kfktport is Apache Kafka transport.
type kfktport struct {
	sync.Mutex
	opts transport.Options
	// inflight keeps track of active requests (stored with correlationID)
	inflight map[string]chan *sarama.ConsumerMessage
	once     sync.Once
	// kafka related entities
	client   sarama.Client
	producer sarama.AsyncProducer
	consumer sarama.PartitionConsumer
}

// NewTransport is a constructor func for apache kafka transport.
func NewTransport(opts ...transport.Option) transport.Transport {
	// TODO: update global sarama/cluster configs with provided secure options
	options := newDefaultOptions()
	for _, o := range opts {
		o(&options)
	}
	// kafka transport
	return &kfktport{
		opts:     options,
		inflight: make(map[string]chan *sarama.ConsumerMessage),
	}
}

// String to satisfy fmt.Stringer interface.
func (k *kfktport) String() string { return "kafka" }

// Dial instantiates an RPC client connected to the message queue.
func (k *kfktport) Dial(addr string, opts ...transport.DialOption) (transport.Client, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	// create single client for consumer and producer
	if k.client, err = sarama.NewClient(k.opts.Addrs, newSaramaConfig(k.opts)); err != nil {
		return nil, err
	}
	if k.producer, err = sarama.NewAsyncProducerFromClient(k.client); err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(k.client)
	if err != nil {
		return nil, err
	}
	if k.consumer, err = consumer.ConsumePartition(addr+suffRecv, 0, sarama.OffsetNewest); err != nil {
		return nil, err
	}
	// run transport consumer's loop
	k.once.Do(k.init)
	// kafka transport client
	return &kfktportClient{
		rt:    k,
		addr:  addr,
		corID: id.String(),
		reply: k.putReq(id.String()),
	}, nil
}

// Listen starts listening for the requests comming through the message queue.
func (k *kfktport) Listen(addr string, opts ...transport.ListenOption) (transport.Listener, error) {
	if addr == "" || addr == ":0" {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		addr = id.String()
	}
	// kafka transport listener
	listener := &kfktportListener{
		rt:      k,
		addr:    addr,
		sockets: make(map[string]*kfktportSocket),
	}
	var err error
	if listener.producer, err = sarama.NewAsyncProducer(k.opts.Addrs, newSaramaConfig(k.opts)); err != nil {
		return nil, err
	}
	// NOTE: use the same consumer group ID for all workers to avoid processing message
	// by all available workers at the same time. Only one consumer of the group should
	// be able to receive and process a message.
	if listener.consumer, err = cluster.NewConsumer(k.opts.Addrs, addr, []string{addr + suffSend}, newClusterConfig(k.opts)); err != nil {
		return nil, err
	}
	return listener, nil
}

// putReq temporrary stores the channel with provided corellation ID.
func (k *kfktport) putReq(id string) chan *sarama.ConsumerMessage {
	k.Lock()
	defer k.Unlock()
	ch := make(chan *sarama.ConsumerMessage, 1)
	k.inflight[id] = ch
	return ch
}

func (k *kfktport) getReq(id string) chan *sarama.ConsumerMessage {
	k.Lock()
	defer k.Unlock()
	if ch, ok := k.inflight[id]; ok {
		return ch
	}
	return nil
}

func (k *kfktport) popReq(id string) {
	k.Lock()
	defer k.Unlock()
	if _, ok := k.inflight[id]; ok {
		delete(k.inflight, id)
	}
}

func (k *kfktport) init() {
	go func() {
		for msg := range k.consumer.Messages() {
			go k.handle(msg)
		}
	}()
}

func (k *kfktport) handle(msg *sarama.ConsumerMessage) {
	ch := k.getReq(string(msg.Key))
	// since clients do not use consumer groups we have to ignore messages addressed
	// to other clients
	if ch == nil {
		return
	}
	ch <- msg
}

func init() { cmd.DefaultTransports["kafka"] = NewTransport }
