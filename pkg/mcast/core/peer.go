package core

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
	"time"
)

// When sending a message the peer must choose
// which kind of message will be emitted.
type emission = uint

const (
	// When emitting a message only internally inside
	// the partition. This is used when the message is
	// on State S0 or S2.
	inner emission = iota

	// Used when exchanging the message Timestamp between
	// the other partitions that participate on the protocol.
	outer
)

// An observer that waits until the issued request
// is committed by one of the peers.
// When the response is committed it will be sent
// back through the observer channel.
type observer struct {
	// Request UID.
	uid types.UID

	// Channel to notify the response back.
	notify chan types.Response
}

// Interface that a single peer must implement.
type PartitionPeer interface {
	// Issues a request to the Generic Multicast protocol.
	//
	// This method does not work in the request-response model
	// so after the message is committed onto the unity
	// a response will be sent back through the channel.
	Command(message types.Message) <-chan types.Response

	// A fast read directly into the storage.
	// Since all peers will be consistent, the read
	// operations can be done directly into the storage.
	//
	// See that if a write was issued, is not guaranteed
	// that the read will be executed after the write.
	FastRead() types.Response

	// Stop the peer.
	Stop()
}

// This structure defines a single peer for the protocol.
// A group of peers will form a single partition, so,
// a single peer is not fault tolerant, but a partition
// will be.
type Peer struct {
	// Mutex for synchronizing operations.
	mutex *sync.Mutex

	// Used to spawn and control all go routines.
	invoker Invoker

	// Holds the observers that are waiting for a response
	// from the issued request.
	observers map[types.UID]observer

	// Configuration for the peer.
	configuration *types.PeerConfiguration

	// Transport used for communication between peers
	// and between partitions.
	transport Transport

	// The peer clock for defining a message timestamp.
	clock LogicalClock

	// The peer received queue, to order the requests.
	rqueue Queue

	// Previous set for the peer.
	previousSet PreviousSet

	// Process responsible to deliver messages on the
	// right order.
	deliver Deliverable

	// Holds the peer log, this will be used
	// for reads only, all writes will come from the
	// state machine when a commit is applied.
	logAbstraction types.Log

	// Conflict relationship for ordering the messages.
	conflict types.ConflictRelationship

	// Peer logger.
	log types.Logger

	// When external requests exchange timestamp,
	// this will hold the received values.
	received *Memo

	// When a message state is updated locally
	// and need to trigger the process again.
	updated chan types.Message

	// The peer cancellable context.
	context context.Context

	// A cancel function to finish the peer processing.
	finish context.CancelFunc
}

// Creates a new peer for the given configuration and
// start polling for new messages.
func NewPeer(configuration *types.PeerConfiguration, clk LogicalClock, log types.Logger) (PartitionPeer, error) {
	t, err := NewTransport(configuration, log)
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithCancel(context.Background())
	logStructure := types.NewLogStructure(configuration.Storage)
	deliver, err := NewDeliver(ctx, log, configuration.Conflict, logStructure)
	if err != nil {
		done()
		return nil, err
	}

	p := &Peer{
		mutex:          &sync.Mutex{},
		observers:      make(map[types.UID]observer),
		invoker:        InvokerInstance(),
		configuration:  configuration,
		transport:      t,
		clock:          clk,
		previousSet:    NewPreviousSet(),
		deliver:        deliver,
		logAbstraction: logStructure,
		conflict:       configuration.Conflict,
		log:            log,
		received:       NewMemo(),
		updated:        make(chan types.Message),
		context:        ctx,
		finish:         done,
	}
	applyDeliver := func(i interface{}, isGenericDeliver bool) {
		p.doDeliver(i.(types.Message), isGenericDeliver)
	}
	p.rqueue = NewQueue(ctx, configuration.Conflict, applyDeliver)
	p.invoker.Spawn(p.poll)
	return p, nil
}

// Implements the PartitionPeer interface.
func (p *Peer) Command(message types.Message) <-chan types.Response {
	res := make(chan types.Response, 1)
	apply := func() {
		err := p.transport.Broadcast(message)
		if err != nil {
			finalResponse := types.Response{
				Success: false,
				Data:    []types.DataHolder{message.Content},
				Failure: err,
			}

			select {
			case res <- finalResponse:
			case <-time.After(100 * time.Millisecond):
			}
			return
		}

		p.mutex.Lock()
		defer p.mutex.Unlock()
		obs := observer{
			uid:    message.Identifier,
			notify: res,
		}
		p.observers[message.Identifier] = obs
	}
	p.invoker.Spawn(apply)
	return res
}

// Implements the PartitionPeer interface.
func (p *Peer) FastRead() types.Response {
	res := types.Response{
		Success: false,
		Data:    nil,
		Failure: nil,
	}
	data, err := p.logAbstraction.Dump()
	if err != nil {
		res.Success = false
		res.Data = nil
		res.Failure = err
		return res
	}

	res.Success = true
	res.Failure = nil
	for _, message := range data {
		res.Data = append(res.Data, message.Content)
	}
	return res
}

// Implements the PartitionPeer interface.
func (p *Peer) Stop() {
	defer func() {
		close(p.updated)
	}()
	p.finish()
	p.transport.Close()
}

// This method will keep polling as long as the peer
// is active.
// Listening for messages received from the transport
// and processing following the protocol definition.
// If the context is cancelled, this method will stop.
func (p *Peer) poll() {
	defer p.log.Debugf("closing the peer %s", p.configuration.Name)
	for {
		select {
		case <-p.context.Done():
			return
		case m, ok := <-p.updated:
			if !ok {
				return
			}
			p.invoker.Spawn(func() {
				p.send(m, types.Initial, inner)
			})
		case m, ok := <-p.transport.Listen():
			if !ok {
				return
			}
			p.invoker.Spawn(func() {
				p.process(m)
			})
		}
	}
}

// Process the received message from the transport.
// First verify if the current configured peer can handle
// this request version.
//
// If the process can be handled, the message is then processed,
// the message must be of type initial or of type external.
//
// After processing the message, updates the value on the
// received queue and then trigger the deliver method to
// start commit on the state machine.
func (p Peer) process(message types.Message) {
	header := message.Extract()
	if header.ProtocolVersion != p.configuration.Version {
		p.log.Warnf("peer not processing message %#v on version %d", message, header.ProtocolVersion)
		return
	}

	if !p.rqueue.IsEligible(message) {
		return
	}
	enqueue := true
	defer func() {
		if enqueue {
			p.finishMessageProcessing(&message)
		}
	}()

	switch header.Type {
	case types.Initial:
		p.log.Debugf("processing internal request %#v", message)
		p.processInitialMessage(&message)
	case types.External:
		p.log.Debugf("processing external request %#v", message)
		enqueue = p.exchangeTimestamp(&message)
	default:
		p.log.Warnf("unknown message type %d", header.Type)
		enqueue = false
	}
}

// After the process GB-Deliver m, if m.State is equals to S0, firstly the
// algorithm check if m conflict with any other message on previousSet,
// if so, the process p increment its local clock and empty the previousSet.
// At last, the message m receives its group timestamp and is added to
// previousSet maintaining the information about conflict relations to
// future messages.
//
// On the second part of this procedure, the process p checks if m.Destination
// has only one destination, if so, message m can jump to state S3, since its
// not necessary to exchange timestamp information between others destination
// groups and a second consensus can be avoided due to group timestamp is now a
// final timestamp.
//
// Otherwise, for messages on state S0, we set the group timestamp to the value
// of the process clock, update m.State to S1 and send m to all others groups in
// m.Destination. On the other hand, to messages on state S2, the message has the
// final timestamp, thus m.State can be updated to the final state S3 and, if
// m.Timestamp is greater than local clock value, the clock is updated to hold
// the received timestamp and the previousSet can be cleaned.
func (p *Peer) processInitialMessage(message *types.Message) {
	if message.State == types.S0 {
		if p.conflict.Conflict(*message, p.previousSet.Snapshot()) {
			p.clock.Tick()
			p.previousSet.Clear()
		}
		message.Timestamp = p.clock.Tock()
		p.previousSet.Append(*message)
	}

	if len(message.Destination) > 1 {
		if message.State == types.S0 {
			message.State = types.S1
			message.Timestamp = p.clock.Tock()
			p.received.Insert(message.Identifier, p.configuration.Partition, message.Timestamp)
			p.send(*message, types.External, outer)
		} else if message.State == types.S2 {
			message.State = types.S3
			if message.Timestamp > p.clock.Tock() {
				p.clock.Leap(message.Timestamp)
				p.previousSet.Clear()
			}
		}
	} else {
		message.Timestamp = p.clock.Tock()
		message.State = types.S3
	}
}

// When a message m has more than one destination group, the destination groups
// have to exchange its timestamps to decide the final timestamp to m.
// Thus, after receiving all other timestamp values, a temporary variable tsm is
// agree upon the maximum timestamp value received.
//
// Once the algorithm have select the tsm value, the process checks if m.Timestamp
// is greater or equal to tsm, in positive case, a second consensus instance can be
// avoided and, the state of m can jump directly to state S3 since the group local
// clock is already bigger than tsm.
func (p *Peer) exchangeTimestamp(message *types.Message) bool {
	p.received.Insert(message.Identifier, message.From, message.Timestamp)
	values := p.received.Read(message.Identifier)
	if len(values) < len(message.Destination) {
		return false
	}

	tsm := helper.MaxValue(values)
	if message.Timestamp >= tsm {
		message.State = types.S3
	} else {
		message.Timestamp = tsm
		message.State = types.S2
	}
	return true
}

// Used to send a request using the transport API.
// Used for request across partitions, when exchanging the
// message timestamp or when broadcasting the message internally
// inside a partition.
func (p Peer) send(message types.Message, t types.MessageType, emission emission) {
	message.Header.Type = t
	message.From = p.configuration.Partition
	var destination []types.Partition
	if emission == inner {
		destination = append(destination, p.configuration.Partition)
	} else {
		destination = append(destination, message.Destination...)
	}

	for _, partition := range destination {
		for err := p.transport.Unicast(message, partition); err != nil; {
			p.log.Errorf("error unicast %s to partition %s. %v", message.Identifier, partition, err)
		}
	}
}

// After the message is processed by the protocol, the value
// will be updated on the rqueue, and if the message is on the
// state S0 or S2 it needs to be broadcast internally to the
// partition.
func (p *Peer) finishMessageProcessing(message *types.Message) {
	defer func() {
		recover()
	}()

	if p.rqueue.Enqueue(*message) {
		uid := message.Identifier
		p.invoker.Spawn(func() {
			p.reprocessMessage(uid)
		})
	}
}

// Verify if the given message needs to be resend
// to the processes inside a partition.
// This methods receives the UID instead of the message
// object, so this ensures that the r_queue and the
// protocols see the same object state.
func (p Peer) reprocessMessage(uid types.UID) {
	value := p.rqueue.GetIfExists(string(uid))
	if value == nil {
		return
	}
	message := value.(types.Message)
	if message.State == types.S0 || message.State == types.S2 {
		select {
		case <-p.context.Done():
			return
		case <-time.After(100 * time.Millisecond):
			p.reprocessMessage(uid)
			return
		case p.updated <- message:
			return
		}
	}

	if message.State == types.S3 {
		p.rqueue.GenericDeliver(message)
	}
}

// The doDeliver method to commit the element on the head
// of the rqueue. Since the rqueue will be already sorted,
// both by the timestamp and by the message UID, we have
// the guarantee that when a message on the head is on the
// state S3 it will be the right message to be delivered.
//
// Since a message on state S3 already has its final timestamp,
// and since the message is on the head of the rqueue it also
// contains the lowest timestamp, so the message is ready to
// be delivered, which means, it will be committed on the
// local peer state machine.
func (p *Peer) doDeliver(m types.Message, isGenericDeliver bool) {
	p.received.Remove(m.Identifier)
	res := p.deliver.Commit(m, isGenericDeliver)
	p.invoker.Spawn(func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		obs, ok := p.observers[m.Identifier]
		if ok {
			select {
			case <-time.After(150 * time.Millisecond):
				break
			case obs.notify <- res:
				break
			}
			close(obs.notify)
			delete(p.observers, obs.uid)
		}
	})
}
