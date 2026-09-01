package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// creating a new client
func NewS3Client(endpoint, accessKey, secretKey string) (*s3.Client, error) {
	// create config first
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	// make the actual client with this config
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	});

	return client, nil;
}

func EnsureBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	});

	if err == nil {
		return nil // bucket already exists
	}

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	});
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil;
}

type S3Storage struct {
	client *s3.Client
	bucket string
}

func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{
		client: client,
		bucket: bucket,
	}
}

// save function to match Storage interface
func (s *S3Storage) Save(name string, r io.Reader) (string, error) {
	key := fmt.Sprintf("%s-%s", uuid.NewString(), filepath.Base(name));

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key: aws.String(key),
		Body: r,
	});

	if err != nil {
		return "", fmt.Errorf("failed to upload to s3: %w", err)
	}

	return key, nil
}