package test

import (
	"go-mcast/internal"
	"testing"
	"time"
)

func TestProtocol_BootstrapUnity(t *testing.T) {
	partitionName := internal.Partition("bootstrap-1-unity")
	unity := CreateUnity(partitionName, t)
	unity.Shutdown()
}

func TestProtocol_BootstrapUnityCluster(t *testing.T) {
	cluster := CreateCluster(3, "cluster", t)
	cluster.Off()
}

// When dispatching a message to single unity containing a single
// process, the message will be transferred directly to state S3
// and can be delivered/committed.
// Since only exists a single node, the sequence number will not conflicts
// thus the clock will not be ticked.
//
// Then a response will be queried back from the unity state machine.
func TestProtocol_GMCastMessageSingleUnitySingleProcess(t *testing.T) {
	partitionName := internal.Partition("single-unity")
	unity := CreateUnity(partitionName, t)
	defer unity.Shutdown()
	key := []byte("test-key")
	value := []byte("test")
	write := internal.Request{
		Key:         key,
		Value:       value,
		Destination: []internal.Partition{partitionName},
	}

	id, err := unity.Write(write)
	if err != nil {
		t.Fatalf("failed writing request %v. %v", write, err)
	}

	time.Sleep(time.Second)

	// Now that the write request succeeded the value will
	// be queried back for validation.
	read := internal.Request{
		Key:         key,
		Destination: []internal.Partition{partitionName},
	}

	res, err := unity.Read(read)
	if err != nil {
		t.Fatalf("failed reading value %v. %v", read, err)
	}

	if !res.Success {
		t.Fatalf("read operation failed. %v", res.Failure)
	}

	if id != res.Identifier {
		t.Fatalf("response identifier should be %s but was %s", id, res.Identifier)
	}
}
