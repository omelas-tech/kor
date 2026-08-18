package main

import "testing"

// The listen address is a security control: kord has no authentication, so any
// client that can reach the port has full read/write on every document. These
// cases are the difference between a private database and a public one.
func TestRequireLoopback(t *testing.T) {
	allowed := []string{
		"127.0.0.1:6565",
		"127.0.0.1:0",
		"localhost:6565",
		"[::1]:6565",
		"127.0.0.53:6565", // anything in 127/8 is loopback
	}
	for _, addr := range allowed {
		if err := requireLoopback(addr); err != nil {
			t.Errorf("requireLoopback(%q) = %v, want nil", addr, err)
		}
	}

	refused := []string{
		"0.0.0.0:6565",  // every interface, including the public one
		"[::]:6565",     // same, v6
		":6565",         // empty host binds everything — the easy mistake
		"10.0.0.5:6565", // private network is still off-host
		"203.0.113.7:6565",
	}
	for _, addr := range refused {
		if err := requireLoopback(addr); err == nil {
			t.Errorf("requireLoopback(%q) = nil, want an error — this address is reachable off-host", addr)
		}
	}

	if err := requireLoopback("not-an-address"); err == nil {
		t.Error("an unparseable listen address must be refused, not assumed safe")
	}
}

func TestIsLoopbackHost(t *testing.T) {
	for _, h := range []string{"127.0.0.1", "::1", "localhost"} {
		if !isLoopbackHost(h) {
			t.Errorf("isLoopbackHost(%q) = false, want true", h)
		}
	}
	for _, h := range []string{"0.0.0.0", "10.0.0.5", "example.com"} {
		if isLoopbackHost(h) {
			t.Errorf("isLoopbackHost(%q) = true, want false", h)
		}
	}
}
