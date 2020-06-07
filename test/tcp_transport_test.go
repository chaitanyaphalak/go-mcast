package test

import (
	"go-mcast/pkg/mcast"
	"net"
	"os"
	"testing"
)

// Fails with advertisable address
func TestTCPTransport_BadAddress(t *testing.T) {
	_, err := mcast.NewTCPTransport("0.0.0.0:0", nil, 1, 0, os.Stdout)
	if err != mcast.ErrorNotAdvertiseAddress {
		t.Fatalf("err: %v", err)
	}
}

// Test that the advertised address is the current local address
func TestTCPTransport_WithAdvertiseAddress(t *testing.T) {
	addr := &net.TCPAddr{
		IP:   []byte{127, 0, 0, 1},
		Port: 56700,
	}
	trans, err := mcast.NewTCPTransport("0.0.0.0:0", addr, 1, 0, os.Stdout)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if trans.LocalAddress() != "127.0.0.1:56700" {
		t.Fatalf("not advertised: %s", trans.LocalAddress())
	}
}