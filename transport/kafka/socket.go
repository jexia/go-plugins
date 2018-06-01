package kafka

import (
	"errors"
	"io"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/micro/go-micro/transport"
)

// kfktportSocket is Apache Kafka socket.
type kfktportSocket struct {
	sync.Mutex
	rt      *kfktport
	addr    string
	replyTo string

	send  chan<- *sarama.ProducerMessage
	recv  chan *sarama.ConsumerMessage
	close chan struct{}
	bl    []*sarama.ConsumerMessage
}

// Recv reads a message from the socket.
func (s *kfktportSocket) Recv(m *transport.Message) error {
	if m == nil {
		return errors.New("message passed in is nil")
	}

	d, ok := <-s.recv
	if !ok {
		return io.EOF
	}

	s.Lock()
	if len(s.bl) > 0 {
		select {
		case s.recv <- s.bl[0]:
			s.bl = s.bl[1:]
		default:
		}
	}
	s.Unlock()

	mr := &transport.Message{
		Header: make(map[string]string),
		Body:   d.Value,
	}
	for _, h := range d.Headers {
		mr.Header[string(h.Key)] = string(h.Value)
	}
	*m = *mr

	return nil
}

// Send message to the socket.
func (s *kfktportSocket) Send(m *transport.Message) error {
	if m == nil {
		return errors.New("message passed in is nil")
	}
	defer s.Close()

	msg := &sarama.ProducerMessage{
		Topic: s.addr + suffRecv,
		Key:   sarama.StringEncoder(s.replyTo),
		Value: sarama.StringEncoder(m.Body),
	}
	for k, v := range m.Header {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	s.send <- msg

	return nil
}

// Close the socket.
func (s *kfktportSocket) Close() error {
	select {
	case <-s.close:
		break
	default:
		close(s.close)
	}
	return nil
}
