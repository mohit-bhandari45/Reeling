package queue

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func NewConn(url string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url);
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := jetstream.New(nc);
	if err != nil {
		return nil, nil, fmt.Errorf("failed to init jetstream: %w", err)
	}

	return nc, js, nil
}