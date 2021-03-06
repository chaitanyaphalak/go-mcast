package fuzzy

import (
	"github.com/jabolina/go-mcast/test"
	"go.uber.org/goleak"
	"log"
	"sync"
	"testing"
	"time"
)

// This test will emit a command a time, iterating over
// a string slice containing the alphabet.
// This is only to verify if after a sequence of commands
// all partitions will end at the same state, since in this
// test no failure is injected over the transport.
func Test_SequentialCommands(t *testing.T) {
	cluster := test.CreateCluster(3, "alphabet", t)
	defer func() {
		if !test.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			test.PrintStackTrace(t)
		}
		goleak.VerifyNone(t)
	}()

	key := []byte("alphabet")
	for _, letter := range test.Alphabet {
		log.Printf("************************** sending %s **************************", letter)
		req := test.GenerateRequest(key, []byte(letter), cluster.Names)
		obs := cluster.Next().Write(req)
		select {
		case res := <-obs:
			if !res.Success {
				t.Errorf("failed writting request %v", res.Failure)
				break
			}
		case <-time.After(3 * time.Second):
			t.Errorf("write %s timeout %#v", letter, req)
			break
		}
	}

	time.Sleep(10 * time.Second)
	cluster.DoesClusterMatchTo(key, []byte("Z"))
}

func Test_ConcurrentCommands(t *testing.T) {
	cluster := test.CreateCluster(3, "concurrent", t)
	defer func() {
		if !test.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			test.PrintStackTrace(t)
		}
		goleak.VerifyNone(t)
	}()

	key := []byte("alphabet")
	group := sync.WaitGroup{}
	write := func(idx int, val string) {
		defer group.Done()
		u := cluster.Next()
		log.Printf("************************** sending %s **************************", val)
		req := test.GenerateRequest(key, []byte(val), cluster.Names)
		res := <-u.Write(req)
		if !res.Success {
			t.Errorf("failed writting request %v", res.Failure)
		}
	}

	for i, content := range test.Alphabet {
		group.Add(1)
		go write(i, content)
	}

	if !test.WaitThisOrTimeout(group.Wait, 30*time.Second) {
		t.Errorf("not finished all after 30 seconds!")
	} else {
		time.Sleep(10 * time.Second)
		cluster.DoesAllClusterMatch(key)
	}
}
