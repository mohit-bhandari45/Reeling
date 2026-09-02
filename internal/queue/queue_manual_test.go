package queue

import (
	"context"
	"testing"
)

func TestNatsConnManual(t *testing.T) {
	nc, _, err := NewConn("nats://localhost:4222")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
}

func TestEnsureStreamManual(t *testing.T) {
	nc, js, err := NewConn("nats://localhost:4222")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	ctx := context.Background()
	if err := EnsureStream(ctx, js); err != nil {
		t.Fatal(err)
	}
}

func TestListStreamsManual(t *testing.T) {
	nc, js, err := NewConn("nats://localhost:4222")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	ctx := context.Background()
	streams := js.ListStreams(ctx)
	for stream := range streams.Info() {
		t.Logf("found stream: %s", stream.Config.Name)
	}
}