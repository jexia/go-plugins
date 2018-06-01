package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/micro/go-micro/transport"
)

type kfktportClient struct {
	rt    *kfktport
	corID string
	addr  string
	reply chan *sarama.ConsumerMessage
}

// Send message to the queue.
func (c *kfktportClient) Send(m *transport.Message) error {
	msg := &sarama.ProducerMessage{
		Topic: c.addr + suffSend,
		Key:   sarama.StringEncoder(c.corID),
		Value: sarama.StringEncoder(m.Body),
	}
	for k, v := range m.Header {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	c.rt.producer.Input() <- msg
	return nil
}

// Recv reads a message from a queue addressed to the current client.
func (c *kfktportClient) Recv(m *transport.Message) error {
	d := <-c.reply
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

// Close client removing its channel from transport inflight.
func (c *kfktportClient) Close() error {
	c.rt.popReq(c.corID)
	return nil
}
