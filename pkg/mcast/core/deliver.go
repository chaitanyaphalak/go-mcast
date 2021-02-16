package core

import (
	"context"
	"errors"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

var (
	ErrCommandUnknown = errors.New("unknown command applied into state machine")
)

// Interface to deliver messages.
type Deliverable interface {
	// Commit the given message on the state machine.
	Commit(message types.Message, isGenericDelivery bool) types.Response
}

// A struct that is able to deliver message from the protocol.
// The messages will be committed on the peer state machine
// and a notification will be generated,
type Deliver struct {
	// Parent context of the delivery.
	// The parent who instantiate the delivery is the peer that
	// relies inside a partition, so for each peer will exists a
	// deliver instance.
	// When the peer is shutdown, also will be shutdown the deliver.
	ctx context.Context

	// Conflict relationship to order the messages.
	conflict types.ConflictRelationship

	// The peer state machine.
	sm types.StateMachine

	// Deliver logger.
	log types.Logger
}

// Creates a new instance of the Deliverable interface.
func NewDeliver(ctx context.Context, log types.Logger, conflict types.ConflictRelationship, logStructure types.Log) (Deliverable, error) {
	sm := types.NewStateMachine(logStructure)
	if err := sm.Restore(); err != nil {
		return nil, err
	}
	d := &Deliver{
		ctx:      ctx,
		conflict: conflict,
		sm:       sm,
		log:      log,
	}
	return d, nil
}

// Commit the message on the peer state machine.
// After the commit a notification is sent through the commit channel.
// The committed message will be passed through the StateMachine, to be applied
// to both the Log and the Storage.
// After applying the Message a response must be sent back to the client, using a
// Listener interface.
func (d Deliver) Commit(m types.Message, isGenericDelivery bool) types.Response {
	res := types.Response{
		Success: false,
		Data:    nil,
		Failure: nil,
	}
	d.log.Debugf("commit request %#v", m)
	err := d.sm.Commit(m, isGenericDelivery)
	if err != nil {
		d.log.Errorf("failed to commit %#v. %v", m, err)
		res.Success = false
		res.Failure = err
		return res
	}

	res.Success = true
	res.Failure = nil
	switch m.Content.Operation {
	case types.Command:
		res.Data = []types.DataHolder{m.Content}
	case types.Query:
		messages, err := d.sm.History()
		if err != nil {
			res.Success = false
			res.Data = nil
			res.Failure = err
		} else {
			for _, message := range messages {
				res.Data = append(res.Data, message.Content)
			}
		}
	default:
		res.Success = false
		res.Data = nil
		res.Failure = ErrCommandUnknown
	}

	return res
}
