package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/definition"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"testing"
)

func TestLog_AppendAndRead(t *testing.T) {
	log := types.NewLogStructure(definition.NewDefaultStorage())
	var uids []types.UID
	for i := 0; i < 10; i++ {
		uid := types.UID(helper.GenerateUID())
		msg := types.Message{
			Timestamp: uint64(i),
			Identifier: uid,
		}
		log.Append(msg, false)
		uids = append(uids, uid)
	}

	if log.Size() != 10 {
		t.Errorf("Expected 10 operations found %d", log.Size())
	}

	messages, err := log.Dump()
	if err != nil {
		t.Errorf("Failed reading message. %v", err)
	}

	if len(messages) != len(uids) {
		t.Errorf("expected %d messages, found %d", len(uids), len(messages))
	}

	for i, message := range messages {
		if message.Identifier != uids[i] {
			t.Errorf("expected UIDS %s, found %s", uids[i], message.Identifier)
		}
	}
}
