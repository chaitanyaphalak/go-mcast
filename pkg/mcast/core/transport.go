package core

import (
	"context"
	"encoding/json"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/relt/pkg/relt"
	"github.com/prometheus/common/log"
	"time"
)

// The transport interface providing the communication
// primitives by the protocol.
type Transport interface {
	// Reliably deliver the message to all correct processes
	// in the same order.
	Broadcast(message types.Message) error

	// Unicast the message to a single partition.
	// This do not need to be a reliable transport, since
	// a partition contains a majority of correct processes
	// at least 1 process will receive the message.
	Unicast(message types.Message, partition types.Partition) error

	// Listen for messages that arrives on the transport.
	Listen() <-chan types.Message

	// Close the transport for sending and receiving messages.
	Close()
}

// An instance of the Transport interface that
// provides the required reliable transport primitives.
type ReliableTransport struct {
	// Transport logger.
	log types.Logger

	// Reliable transport.
	relt *relt.Relt

	// Channel to publish the receiving messages.
	producer chan types.Message

	// The transport context.
	context context.Context

	// The finish function to closing the transport.
	finish context.CancelFunc

	partition string
}

// Create a new instance of the transport interface.
func NewTransport(peer *types.PeerConfiguration, log types.Logger) (Transport, error) {
	conf := relt.DefaultReltConfiguration()
	conf.Name = peer.Name
	conf.Exchange = relt.GroupAddress(peer.Partition)
	r, err := relt.NewRelt(*conf)
	if err != nil {
		return nil, err
	}
	ctx, done := context.WithCancel(context.Background())
	t := &ReliableTransport{
		log:      log,
		relt:     r,
		producer: make(chan types.Message, 100),
		partition: peer.Name,
		context:  ctx,
		finish:   done,
	}
	InvokerInstance().Spawn(t.poll)
	return t, nil
}

func (r *ReliableTransport) apply(message types.Message, partition types.Partition) error {
	data, err := json.Marshal(message)
	if err != nil {
		log.Errorf("failed marshalling unicast message %#v. %v", message, err)
	}

	m := relt.Send{
		Address: relt.GroupAddress(partition),
		Data:    data,
	}
	return r.relt.Broadcast(r.context, m)
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Broadcast(message types.Message) error {
	for _, partition := range message.Destination {
		if err := r.apply(message, partition); err != nil {
			r.log.Errorf("failed sending %#v. %v", m, err)
			return err
		}
	}
	return nil
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Unicast(message types.Message, partition types.Partition) error {
	return r.apply(message, partition)
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Listen() <-chan types.Message {
	return r.producer
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Close() {
	r.finish()
	if err := r.relt.Close(); err != nil {
		r.log.Errorf("failed stopping transport. %#v", err)
	}
}

// This method will keep polling until
// the transport context cancelled.
// The messages that arrives through the underlying
// transport channel will be sent to the consume
// method to be parsed and publish to the listeners.
func (r ReliableTransport) poll() {
	listener, err := r.relt.Consume()
	if err != nil {
		panic(err)
	}
	for {
		select {
		case <-r.context.Done():
			return
		case recv, ok := <-listener:
			if !ok {
				return
			}
			r.consume(recv.Origin, relt.Recv{
				Data:  recv.Data,
				Error: recv.Error,
			})
		}
	}
}

// Consume will receive a message from the transport
// and will parse into a valid object to be consumed
// by the channel listener.
func (r *ReliableTransport) consume(origin string, recv relt.Recv) {
	if recv.Error != nil {
		r.log.Errorf("failed consuming message from %s. %v", origin, recv.Error)
		return
	}

	if recv.Data == nil {
		r.log.Warnf("received empty message from %s", origin)
		return
	}

	var m types.Message
	if err := json.Unmarshal(recv.Data, &m); err != nil {
		r.log.Errorf("failed unmarshalling message %#v. %v", recv, err)
		return
	}
	r.log.Infof("%s received %#v", r.partition, m)
	timeout, cancel := context.WithTimeout(r.context, 250*time.Millisecond)
	defer cancel()
	select {
	case <-timeout.Done():
		r.log.Warnf("failed consuming %#v", m)
		return
	case r.producer <- m:
		return
	}
}
