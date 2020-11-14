package types

import (
	"encoding/json"
	"errors"
)

var (
	ErrCommandUnknown = errors.New("unknown command applied into state machine")
)

// StateMachine is an interface that can be implemented
// to use the replicated value across replicas.
type StateMachine interface {
	// Commit the given entry into the state machine, turning it available for all clients.
	Commit(*Entry) (interface{}, error)

	// Restores the state machine back to a given a state.
	Restore() error
}

// A in memory default value to be used.
type InMemoryStateMachine struct {
	// State machine stable storage for committing
	store Storage
}

// Commit the operation into the stable storage.
// Some operations will change values into the state machine
// while some other operations is just querying the state
// machine for values.
func (i *InMemoryStateMachine) Commit(entry *Entry) (interface{}, error) {
	storage := StorageEntry{Key: entry.Identifier, Value: entry.Data, Type: entry.Operation}
	switch entry.Operation {
	// Some entry will be changed.
	case Command:
		if err := i.store.Set(storage); err != nil {
			return nil, err
		}
		return entry, nil
	// Read an entry.
	case Query:
		data, err := i.store.Get()
		if err != nil {
			return nil, err
		}
		var response []Message
		for _, stored := range data {
			var message Message
			if err := json.Unmarshal(stored.Value, &message); err != nil {
				return nil, err
			}
			response = append(response, message)
		}
		return response, nil
	default:
		return nil, ErrCommandUnknown
	}
}

func (i *InMemoryStateMachine) Restore() error {
	return nil
}

// Create the new state machine using the given storage
// for committing changes.
func NewStateMachine(storage Storage) *InMemoryStateMachine {
	return &InMemoryStateMachine{store: storage}
}
