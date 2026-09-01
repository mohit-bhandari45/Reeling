package storage

import (
	"context"
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