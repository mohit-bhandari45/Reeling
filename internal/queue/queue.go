package queue

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName = "JOBS"
	SubjectName = "jobs.transcode"
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

func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(
		ctx,
		jetstream.StreamConfig{
			Name: StreamName,
			Subjects: []string{SubjectName},
		},
	);

	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil;
}