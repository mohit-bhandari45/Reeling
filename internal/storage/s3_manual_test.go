package storage

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3ConnManual(t *testing.T) {
	client, err := NewS3Client("http://localhost:9000", "reeling", "reelingreeling")
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestS3SaveManual(t *testing.T) {
	client, err := NewS3Client("http://localhost:9000", "reeling", "reelingreeling")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := EnsureBucket(ctx, client, "reeling-videos"); err != nil {
		t.Fatal(err)
	}

	s3Storage := NewS3Storage(client, "reeling-videos")

	file, err := os.Open("../../sample.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	key, err := s3Storage.Save("sample.mp4", file)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("uploaded with key: %s", key)
}

func TestS3OpenManual(t *testing.T) {
	client, err := NewS3Client("http://localhost:9000", "reeling", "reelingreeling")
	if err != nil {
		t.Fatal(err)
	}

	s3Storage := NewS3Storage(client, "reeling-videos")

	// use the key printed by TestS3SaveManual's t.Logf output
	reader, err := s3Storage.Open("6ee8e791-a6fb-4902-8d90-1f884f3f219c-sample.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	out, err := os.Create("downloaded.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	n, err := io.Copy(out, reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("downloaded %d bytes", n)
}