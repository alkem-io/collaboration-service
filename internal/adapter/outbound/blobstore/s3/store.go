// Package s3 is the S3-compatible BlobStore (port.BlobStore) for standalone
// deployments: the encoded Y.Doc v2 snapshot is stored as an object whose key is
// the content pointer (the document id, optionally under a configured prefix).
// It targets any S3-compatible endpoint (AWS S3, MinIO, localstack) via the
// aws-sdk-go-v2 client.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Config carries the S3 connection settings.
type Config struct {
	// Bucket is the destination bucket (required).
	Bucket string
	// Region is the AWS region (required unless Endpoint is set, e.g. MinIO).
	Region string
	// Endpoint overrides the AWS endpoint for an S3-compatible store
	// (MinIO/localstack); empty uses the default AWS endpoint resolver.
	Endpoint string
	// KeyPrefix is prepended to every object key (e.g. "collaboration/"); the
	// content pointer the metadata records stays prefix-free so it is portable.
	KeyPrefix string
	// AccessKeyID / SecretAccessKey override ambient credentials (MinIO); empty
	// falls back to the default credential chain.
	AccessKeyID     string
	SecretAccessKey string
	// UsePathStyle forces path-style addressing (required by MinIO).
	UsePathStyle bool
}

// s3API is the narrow subset of the S3 client the store uses, so tests can fake
// it without a network and the adapter does not depend on the full client type.
type s3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Store persists snapshots as S3 objects.
type Store struct {
	api    s3API
	bucket string
	prefix string
}

// New constructs an S3 blob store from configuration, building the aws-sdk-go-v2
// client (honouring a custom endpoint and credentials for MinIO/localstack).
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 blob store: Bucket is required")
	}
	if cfg.Region == "" && cfg.Endpoint == "" {
		return nil, fmt.Errorf("s3 blob store: Region (or Endpoint) is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
		}
		o.UsePathStyle = cfg.UsePathStyle
	})
	return newWithAPIAndPrefix(client, cfg.Bucket, cfg.KeyPrefix), nil
}

// newWithAPI wires a store over a custom S3 API (tests).
func newWithAPI(api s3API, bucket string) *Store {
	return newWithAPIAndPrefix(api, bucket, "")
}

// newWithAPIAndPrefix wires a store over a custom S3 API with a key prefix.
func newWithAPIAndPrefix(api s3API, bucket, prefix string) *Store {
	return &Store{api: api, bucket: bucket, prefix: prefix}
}

// key maps a content pointer to the object key (applying the prefix).
func (s *Store) key(pointer string) string { return s.prefix + pointer }

// Put stores the snapshot under the object key derived from pointer and echoes
// the pointer back (S3 objects are addressed by the stable key the caller
// supplies — the document id).
func (s *Store) Put(ctx context.Context, pointer string, data []byte) (string, error) {
	k := s.key(pointer)
	_, err := s.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &k,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", fmt.Errorf("s3 put: %w", err)
	}
	return pointer, nil
}

// Get fetches the snapshot for pointer, mapping a missing key to
// model.ErrNotFound.
func (s *Store) Get(ctx context.Context, pointer string) ([]byte, error) {
	k := s.key(pointer)
	out, err := s.api.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &k})
	if err != nil {
		if isNoSuchKey(err) {
			return nil, model.ErrNotFound
		}
		return nil, fmt.Errorf("s3 get: %w", err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 body: %w", err)
	}
	return data, nil
}

// Delete removes the object for pointer. S3 DeleteObject is idempotent (deleting
// an absent key succeeds), so no special not-found handling is needed.
func (s *Store) Delete(ctx context.Context, pointer string) error {
	k := s.key(pointer)
	if _, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &k}); err != nil {
		return fmt.Errorf("s3 delete: %w", err)
	}
	return nil
}

// isNoSuchKey reports whether err is S3's missing-object error (NoSuchKey or a
// 404), which the port maps to model.ErrNotFound.
func isNoSuchKey(err error) bool {
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound" || strings.Contains(code, "404")
	}
	return false
}

// compile-time assertion that Store satisfies the port.
var _ port.BlobStore = (*Store)(nil)
