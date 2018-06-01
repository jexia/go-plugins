package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/micro/go-micro/transport"
)

type kfktportListener struct {
	sync.RWMutex
	rt      *kfktport
	addr    string
	sockets map[string]*kfktportSocket
	// NOTE: do not use the same sarama.Client for the listener's producer and
	// consumer (it is much slower)
	producer sarama.AsyncProducer
	consumer *cluster.Consumer
}

// Addr returns kafka topic name without suffix (-send/-recv).
func (l *kfktportListener) Addr() string {
	return l.addr
}

// Close listener gracefully.
func (l *kfktportListener) Close() error {
	if l.producer != nil {
		l.producer.Close()
	}
	if l.consumer != nil {
		l.consumer.Close()
	}
	return nil
}

// Accept connection to the message queue with provided socket handler func.
func (l *kfktportListener) Accept(fn func(transport.Socket)) (err error) {
	for d := range l.consumer.Messages() {
		l.RLock()
		sock, ok := l.sockets[string(d.Key)]
		l.RUnlock()
		if !ok {
			sock = &kfktportSocket{
				rt:      l.rt,
				addr:    l.addr,
				send:    l.producer.Input(),
				recv:    make(chan *sarama.ConsumerMessage, 1),
				close:   make(chan struct{}),
				replyTo: string(d.Key),
			}

			l.Lock()
			l.sockets[string(d.Key)] = sock
			l.Unlock()
			// delete socket from the pool when it is closed
			go func(close chan struct{}, key string) {
				<-sock.close
				l.Lock()
				delete(l.sockets, key)
				l.Unlock()
			}(sock.close, string(d.Key))
			// call socket handler
			go fn(sock)
		}

		select {
		case <-sock.close:
			continue
		default:
		}

		sock.Lock()
		sock.bl = append(sock.bl, d)
		select {
		case sock.recv <- sock.bl[0]:
			sock.bl = sock.bl[1:]
		default:
		}
		sock.Unlock()
	}
	return nil
}
