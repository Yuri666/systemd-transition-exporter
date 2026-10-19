package sip

import (
	"net"
	"testing"
)

func TestRequireLocalAcceptsAssignedAddress(t *testing.T) {
	locals := []net.IP{net.ParseIP("10.12.12.10"), net.ParseIP("127.0.0.1")}
	if err := RequireLocal("10.12.12.10:5060", locals); err != nil {
		t.Fatal(err)
	}
	mapped := []net.IP{net.ParseIP("::ffff:10.12.12.10")}
	if err := RequireLocal("10.12.12.10:5060", mapped); err != nil {
		t.Fatal(err)
	}
}

func TestRequireLocalRejectsForeignAddress(t *testing.T) {
	locals := []net.IP{net.ParseIP("10.12.12.11"), net.ParseIP("127.0.0.1")}
	if err := RequireLocal("10.12.12.10:5060", locals); err == nil {
		t.Fatal("foreign address was accepted")
	}
	if err := RequireLocal("cscf.example:5060", locals); err == nil {
		t.Fatal("hostname was accepted")
	}
}

func TestLocalIPsIncludesLoopback(t *testing.T) {
	ips, err := LocalIPs()
	if err != nil {
		t.Fatal(err)
	}
	if err := RequireLocal("127.0.0.1:5060", ips); err != nil {
		t.Fatalf("loopback missing from local addresses: %v", err)
	}
}
