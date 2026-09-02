package queue

import "testing"

func TestNatsConnManual(t *testing.T) {
	nc, _, err := NewConn("nats://localhost:4222")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
}