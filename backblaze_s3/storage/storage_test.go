package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"testing"

	connectorstorage "github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/minio/minio-go/v7"
)

type mockMinioClient struct {
	bucketExistsFn  func(ctx context.Context, bucketName string) (bool, error)
	makeBucketFn    func(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error
	statObjectFn    func(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	putObjectFn     func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	getObjectFn     func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	getObjectReadFn func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	listObjectsFn   func(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	removeObjectFn  func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
}

func (m *mockMinioClient) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	if m.bucketExistsFn != nil {
		return m.bucketExistsFn(ctx, bucketName)
	}
	return false, nil
}

func (m *mockMinioClient) MakeBucket(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error {
	if m.makeBucketFn != nil {
		return m.makeBucketFn(ctx, bucketName, opts)
	}
	return nil
}

func (m *mockMinioClient) StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	if m.statObjectFn != nil {
		return m.statObjectFn(ctx, bucketName, objectName, opts)
	}
	return minio.ObjectInfo{}, nil
}

func (m *mockMinioClient) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	if m.putObjectFn != nil {
		return m.putObjectFn(ctx, bucketName, objectName, reader, objectSize, opts)
	}
	return minio.UploadInfo{}, nil
}

func (m *mockMinioClient) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	if m.getObjectFn != nil {
		return m.getObjectFn(ctx, bucketName, objectName, opts)
	}
	return nil, nil
}

func (m *mockMinioClient) GetObjectReader(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	if m.getObjectReadFn != nil {
		return m.getObjectReadFn(ctx, bucketName, objectName, opts)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockMinioClient) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	if m.listObjectsFn != nil {
		return m.listObjectsFn(ctx, bucketName, opts)
	}
	ch := make(chan minio.ObjectInfo)
	close(ch)
	return ch
}

func (m *mockMinioClient) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	if m.removeObjectFn != nil {
		return m.removeObjectFn(ctx, bucketName, objectName, opts)
	}
	return nil
}

func TestNewStore_ErrorsWithoutNetwork(t *testing.T) {
	t.Parallel()

	baseConfig := map[string]string{
		"location":          "b2s3://s3.us-west-004.backblazeb2.com/mybucket/prefix",
		"access_key":        "test-access-key",
		"secret_access_key": "test-secret-key",
	}

	tests := []struct {
		name    string
		mutate  func(map[string]string)
		errLike string
	}{
		{
			name: "missing access_key",
			mutate: func(cfg map[string]string) {
				delete(cfg, "access_key")
			},
			errLike: "missing access_key",
		},
		{
			name: "missing secret_access_key",
			mutate: func(cfg map[string]string) {
				delete(cfg, "secret_access_key")
			},
			errLike: "missing secret_access_key",
		},
		{
			name: "invalid sse_customer_key base64",
			mutate: func(cfg map[string]string) {
				cfg["sse_customer_key"] = "not-base64%%%"
			},
			errLike: "invalid sse_customer_key: must be base64-encoded",
		},
		{
			name: "invalid location url parse error",
			mutate: func(cfg map[string]string) {
				cfg["location"] = "%"
			},
			errLike: "parse location",
		},
		{
			name: "missing host in location",
			mutate: func(cfg map[string]string) {
				cfg["location"] = "b2s3:///mybucket/prefix"
			},
			errLike: "failed to parse the location: bucket name or host name are empty",
		},
		{
			name: "missing bucket in location",
			mutate: func(cfg map[string]string) {
				cfg["location"] = "b2s3://s3.us-west-004.backblazeb2.com"
			},
			errLike: "failed to parse the location: bucket name or host name are empty",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := make(map[string]string, len(baseConfig))
			for k, v := range baseConfig {
				cfg[k] = v
			}
			tc.mutate(cfg)

			store, err := NewStore(context.Background(), "b2s3", cfg)
			if err == nil {
				t.Fatalf("expected error, got store=%#v", store)
			}
			if !strings.Contains(err.Error(), tc.errLike) {
				t.Fatalf("expected error containing %q, got %q", tc.errLike, err.Error())
			}
		})
	}
}

func TestStoreRealpath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefixDir string
		input     string
		want      string
	}{
		{
			name:      "prefix directory plus file",
			prefixDir: "/repo/",
			input:     "CONFIG",
			want:      "repo/CONFIG",
		},
		{
			name:      "root prefix strips leading slash",
			prefixDir: "/",
			input:     "CONFIG",
			want:      "CONFIG",
		},
		{
			name:      "nested object path",
			prefixDir: "/repo/",
			input:     "packfiles/ab/0123",
			want:      "repo/packfiles/ab/0123",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &Store{prefixDir: tc.prefixDir}
			got := s.realpath(tc.input)
			if got != tc.want {
				t.Fatalf("realpath(%q) with prefix %q: got %q, want %q", tc.input, tc.prefixDir, got, tc.want)
			}
		})
	}
}

func TestNewStore_NormalizesPrefixForRealpath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		location string
		want     string
	}{
		{
			name:     "location with nested prefix",
			location: "b2s3://s3.us-west-004.backblazeb2.com/mybucket/my/prefix",
			want:     "my/prefix/CONFIG",
		},
		{
			name:     "location without prefix",
			location: "b2s3://s3.us-west-004.backblazeb2.com/mybucket",
			want:     "CONFIG",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := map[string]string{
				"location":          tc.location,
				"access_key":        "test-access-key",
				"secret_access_key": "test-secret-key",
			}

			store, err := NewStore(context.Background(), "b2s3", cfg)
			if err != nil {
				t.Fatalf("NewStore() unexpected error: %v", err)
			}

			s, ok := store.(*Store)
			if !ok {
				t.Fatalf("NewStore() returned unexpected type %T", store)
			}

			got := s.realpath("CONFIG")
			if got != tc.want {
				t.Fatalf("realpath(\"CONFIG\") from location %q: got %q, want %q", tc.location, got, tc.want)
			}
		})
	}
}

func TestCreate_UsesExistingBucketAndInitializesPrefix(t *testing.T) {
	var putBucket, putObject string
	var putSize int64
	var putPayload []byte

	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		statObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
			return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}
		},
		putObjectFn: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
			data, err := io.ReadAll(reader)
			if err != nil {
				return minio.UploadInfo{}, err
			}
			putBucket = bucketName
			putObject = objectName
			putSize = objectSize
			putPayload = data
			return minio.UploadInfo{Size: objectSize}, nil
		},
	}

	s := &Store{
		minioClient: m,
		bucket:      "mybucket",
		prefixDir:   "/repo/",
		putObjectOptions: minio.PutObjectOptions{
			SendContentMd5: true,
		},
	}

	config := []byte("config-data")
	err := s.Create(context.Background(), config)
	if err != nil {
		t.Fatalf("Create() unexpected error: %v", err)
	}

	if putBucket != "mybucket" {
		t.Fatalf("PutObject bucket: got %q, want %q", putBucket, "mybucket")
	}
	if putObject != "repo/CONFIG" {
		t.Fatalf("PutObject key: got %q, want %q", putObject, "repo/CONFIG")
	}
	if putSize != int64(len(config)) {
		t.Fatalf("PutObject size: got %d, want %d", putSize, len(config))
	}
	if string(putPayload) != string(config) {
		t.Fatalf("PutObject payload: got %q, want %q", string(putPayload), string(config))
	}
}

func TestCreate_CreatesBucketWhenMissing(t *testing.T) {
	var makeBucketCalled bool

	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return false, nil
		},
		makeBucketFn: func(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error {
			makeBucketCalled = true
			return nil
		},
		statObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
			return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}
		},
		putObjectFn: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
			return minio.UploadInfo{Size: objectSize}, nil
		},
	}

	s := &Store{
		minioClient: m,
		bucket:      "mybucket",
		prefixDir:   "/",
		putObjectOptions: minio.PutObjectOptions{
			SendContentMd5: true,
		},
	}

	err := s.Create(context.Background(), []byte("cfg"))
	if err != nil {
		t.Fatalf("Create() unexpected error: %v", err)
	}
	if !makeBucketCalled {
		t.Fatalf("expected MakeBucket to be called")
	}
}

func TestCreate_AlreadyInitialized(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		statObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
			return minio.ObjectInfo{}, nil
		},
		putObjectFn: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
			t.Fatalf("PutObject should not be called when repository is already initialized")
			return minio.UploadInfo{}, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
	err := s.Create(context.Background(), []byte("cfg"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bucket already initialized") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreate_StatObjectUnexpectedError(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		statObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
			return minio.ObjectInfo{}, errors.New("boom")
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
	err := s.Create(context.Background(), []byte("cfg"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "stat object CONFIG") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type closeTrackerReadCloser struct {
	reader io.Reader
	closed bool
}

func (r *closeTrackerReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *closeTrackerReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestOpen_ReadsConfigAndClosesStream(t *testing.T) {
	stream := &closeTrackerReadCloser{reader: strings.NewReader("config-data")}

	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		getObjectReadFn: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
			if bucketName != "mybucket" {
				t.Fatalf("GetObjectReader bucket: got %q, want %q", bucketName, "mybucket")
			}
			if objectName != "repo/CONFIG" {
				t.Fatalf("GetObjectReader key: got %q, want %q", objectName, "repo/CONFIG")
			}
			return stream, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	data, err := s.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() unexpected error: %v", err)
	}
	if string(data) != "config-data" {
		t.Fatalf("Open() data: got %q, want %q", string(data), "config-data")
	}
	if !stream.closed {
		t.Fatalf("expected stream to be closed")
	}
}

func TestOpen_BucketExistsError(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return false, errors.New("boom")
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	_, err := s.Open(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "error checking if bucket exists") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_BucketMissing(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return false, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	_, err := s.Open(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bucket does not exist") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_GetObjectError(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		getObjectReadFn: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
			return nil, errors.New("get fail")
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	_, err := s.Open(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "error getting object") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_MissingConfigReturnsNotExist(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		getObjectReadFn: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
			return nil, minio.ErrorResponse{Code: "NoSuchKey", Message: "Key not found"}
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	_, err := s.Open(context.Background())
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

type errorReadCloser struct{}

func (r *errorReadCloser) Read(p []byte) (int, error) {
	return 0, errors.New("read fail")
}

func (r *errorReadCloser) Close() error {
	return nil
}

func TestOpen_ReadError(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		getObjectReadFn: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
			return &errorReadCloser{}, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	_, err := s.Open(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "error reading object") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type noSuchKeyReadCloser struct{}

func (r *noSuchKeyReadCloser) Read(p []byte) (int, error) {
	return 0, minio.ErrorResponse{Code: "NoSuchKey", Message: "Key not found"}
}

func (r *noSuchKeyReadCloser) Close() error {
	return nil
}

func TestOpen_MissingConfigOnReadReturnsNotExist(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return true, nil
		},
		getObjectReadFn: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
			return &noSuchKeyReadCloser{}, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}

	_, err := s.Open(context.Background())
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

func TestPing_Success(t *testing.T) {
	called := false

	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			called = true
			if bucketName != "mybucket" {
				t.Fatalf("BucketExists bucket: got %q, want %q", bucketName, "mybucket")
			}
			return true, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket"}
	err := s.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping() unexpected error: %v", err)
	}
	if !called {
		t.Fatalf("expected BucketExists to be called")
	}
}

func TestPing_BucketExistsError(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return false, errors.New("boom")
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket"}
	err := s.Ping(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPing_BucketMissing(t *testing.T) {
	m := &mockMinioClient{
		bucketExistsFn: func(ctx context.Context, bucketName string) (bool, error) {
			return false, nil
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket"}
	err := s.Ping(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bucket does not exist") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreMetadataUtils(t *testing.T) {
	t.Parallel()

	s := &Store{
		host:      "s3.us-west-004.backblazeb2.com",
		bucket:    "mybucket",
		prefixDir: "/repo/",
	}

	if got := s.Origin(); got != "s3.us-west-004.backblazeb2.com" {
		t.Fatalf("Origin(): got %q, want %q", got, "s3.us-west-004.backblazeb2.com")
	}

	if got := s.Root(); got != "/mybucket/repo" {
		t.Fatalf("Root(): got %q, want %q", got, "/mybucket/repo")
	}

	if got := s.Type(); got != "b2s3" {
		t.Fatalf("Type(): got %q, want %q", got, "b2s3")
	}

	if got := s.Flags(); got != location.Flags(0) {
		t.Fatalf("Flags(): got %v, want %v", got, location.Flags(0))
	}
}

func TestMode_ReadWrite(t *testing.T) {
	t.Parallel()

	s := &Store{}
	mode, err := s.Mode(context.Background())
	if err != nil {
		t.Fatalf("Mode() unexpected error: %v", err)
	}

	want := connectorstorage.ModeRead | connectorstorage.ModeWrite
	if mode != want {
		t.Fatalf("Mode(): got %v, want %v", mode, want)
	}
}

func TestSize_Unknown(t *testing.T) {
	t.Parallel()

	s := &Store{}
	size, err := s.Size(context.Background())
	if err != nil {
		t.Fatalf("Size() unexpected error: %v", err)
	}
	if size != -1 {
		t.Fatalf("Size(): got %d, want %d", size, -1)
	}
}

func makeTestMAC(seed byte) objects.MAC {
	b := make([]byte, 32)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return objects.MAC(b)
}

func TestList_PackfilesSuccess(t *testing.T) {
	t.Parallel()

	mac1 := makeTestMAC(0x01)
	mac2 := makeTestMAC(0x80)

	m := &mockMinioClient{
		listObjectsFn: func(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			if bucketName != "mybucket" {
				t.Fatalf("ListObjects bucket: got %q, want %q", bucketName, "mybucket")
			}
			if opts.Prefix != "repo/packfiles/" {
				t.Fatalf("ListObjects prefix: got %q, want %q", opts.Prefix, "repo/packfiles/")
			}
			if !opts.Recursive {
				t.Fatalf("ListObjects Recursive: got false, want true")
			}

			ch := make(chan minio.ObjectInfo, 3)
			ch <- minio.ObjectInfo{Key: fmt.Sprintf("repo/packfiles/%02x/%x", mac1[0], mac1[:])}
			ch <- minio.ObjectInfo{Key: fmt.Sprintf("repo/packfiles/%02x/%x", mac2[0], mac2[:])}
			// Wrong length after decode; should be skipped.
			ch <- minio.ObjectInfo{Key: "repo/packfiles/aa/abcd"}
			close(ch)
			return ch
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
	got, err := s.List(context.Background(), connectorstorage.StorageResourcePackfile)
	if err != nil {
		t.Fatalf("List() unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List() len: got %d, want %d", len(got), 2)
	}
	if got[0] != mac1 || got[1] != mac2 {
		t.Fatalf("List() unexpected MACs")
	}
}

func TestList_DecodeError(t *testing.T) {
	t.Parallel()

	m := &mockMinioClient{
		listObjectsFn: func(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo, 1)
			ch <- minio.ObjectInfo{Key: "repo/states/aa/not-hex"}
			close(ch)
			return ch
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
	_, err := s.List(context.Background(), connectorstorage.StorageResourceState)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "decode state key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestList_UnsupportedResource(t *testing.T) {
	t.Parallel()

	s := &Store{minioClient: &mockMinioClient{}}
	_, err := s.List(context.Background(), connectorstorage.StorageResource(999))
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}

func TestPut_CoreResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		res        connectorstorage.StorageResource
		prefixDir  string
		mac        objects.MAC
		payload    string
		wantKey    string
		wantSizeIn int64
		retSize    int64
	}{
		{
			name:       "packfile",
			res:        connectorstorage.StorageResourcePackfile,
			prefixDir:  "/repo/",
			mac:        makeTestMAC(0x1a),
			payload:    "pack-data",
			wantSizeIn: int64(len("pack-data")),
			retSize:    123,
		},
		{
			name:       "state",
			res:        connectorstorage.StorageResourceState,
			prefixDir:  "/repo/",
			mac:        makeTestMAC(0x2b),
			payload:    "state-data",
			wantSizeIn: -1,
			retSize:    456,
		},
		{
			name:       "lock",
			res:        connectorstorage.StorageResourceLock,
			prefixDir:  "/repo/",
			mac:        makeTestMAC(0x3c),
			payload:    "lock-data",
			wantSizeIn: -1,
			retSize:    789,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			switch tc.res {
			case connectorstorage.StorageResourcePackfile:
				tc.wantKey = fmt.Sprintf("repo/packfiles/%02x/%016x", tc.mac[0], tc.mac)
			case connectorstorage.StorageResourceState:
				tc.wantKey = fmt.Sprintf("repo/states/%02x/%016x", tc.mac[0], tc.mac)
			case connectorstorage.StorageResourceLock:
				tc.wantKey = fmt.Sprintf("repo/locks/%016x", tc.mac)
			}

			m := &mockMinioClient{
				putObjectFn: func(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
					if bucketName != "mybucket" {
						t.Fatalf("PutObject bucket: got %q, want %q", bucketName, "mybucket")
					}
					if objectName != tc.wantKey {
						t.Fatalf("PutObject key: got %q, want %q", objectName, tc.wantKey)
					}
					if objectSize != tc.wantSizeIn {
						t.Fatalf("PutObject size arg: got %d, want %d", objectSize, tc.wantSizeIn)
					}
					data, err := io.ReadAll(reader)
					if err != nil {
						return minio.UploadInfo{}, err
					}
					if string(data) != tc.payload {
						t.Fatalf("PutObject payload: got %q, want %q", string(data), tc.payload)
					}
					return minio.UploadInfo{Size: tc.retSize}, nil
				},
			}

			s := &Store{
				minioClient: m,
				bucket:      "mybucket",
				prefixDir:   tc.prefixDir,
				bufPool: sync.Pool{
					New: func() any {
						return &bytes.Buffer{}
					},
				},
			}
			got, err := s.Put(context.Background(), tc.res, tc.mac, strings.NewReader(tc.payload))
			if err != nil {
				t.Fatalf("Put() unexpected error: %v", err)
			}
			if got != tc.retSize {
				t.Fatalf("Put() size: got %d, want %d", got, tc.retSize)
			}
		})
	}
}

func TestPut_UnsupportedResource(t *testing.T) {
	t.Parallel()

	s := &Store{minioClient: &mockMinioClient{}}
	_, err := s.Put(context.Background(), connectorstorage.StorageResource(999), makeTestMAC(0x10), strings.NewReader("x"))
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}

func TestGet_PathAndErrors(t *testing.T) {
	t.Parallel()

	mac := makeTestMAC(0x44)

	tests := []struct {
		name    string
		res     connectorstorage.StorageResource
		wantKey string
		wantErr string
	}{
		{
			name:    "packfile",
			res:     connectorstorage.StorageResourcePackfile,
			wantKey: fmt.Sprintf("repo/packfiles/%02x/%016x", mac[0], mac),
			wantErr: "get packfile object",
		},
		{
			name:    "state",
			res:     connectorstorage.StorageResourceState,
			wantKey: fmt.Sprintf("repo/states/%02x/%016x", mac[0], mac),
			wantErr: "get state object",
		},
		{
			name:    "lock",
			res:     connectorstorage.StorageResourceLock,
			wantKey: fmt.Sprintf("repo/locks/%016x", mac),
			wantErr: "get lock object",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := &mockMinioClient{
				getObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
					if bucketName != "mybucket" {
						t.Fatalf("GetObject bucket: got %q, want %q", bucketName, "mybucket")
					}
					if objectName != tc.wantKey {
						t.Fatalf("GetObject key: got %q, want %q", objectName, tc.wantKey)
					}
					return nil, errors.New("boom")
				},
			}

			s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
			_, err := s.Get(context.Background(), tc.res, mac, nil)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestGet_UnsupportedResource(t *testing.T) {
	t.Parallel()

	s := &Store{minioClient: &mockMinioClient{}}
	_, err := s.Get(context.Background(), connectorstorage.StorageResource(999), makeTestMAC(0x55), nil)
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}

func TestDelete_CoreResources(t *testing.T) {
	t.Parallel()

	mac := makeTestMAC(0x61)
	tests := []struct {
		name    string
		res     connectorstorage.StorageResource
		wantKey string
	}{
		{
			name:    "packfile",
			res:     connectorstorage.StorageResourcePackfile,
			wantKey: fmt.Sprintf("repo/packfiles/%02x/%016x", mac[0], mac),
		},
		{
			name:    "state",
			res:     connectorstorage.StorageResourceState,
			wantKey: fmt.Sprintf("repo/states/%02x/%016x", mac[0], mac),
		},
		{
			name:    "lock",
			res:     connectorstorage.StorageResourceLock,
			wantKey: fmt.Sprintf("repo/locks/%016x", mac),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := &mockMinioClient{
				removeObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
					if bucketName != "mybucket" {
						t.Fatalf("RemoveObject bucket: got %q, want %q", bucketName, "mybucket")
					}
					if objectName != tc.wantKey {
						t.Fatalf("RemoveObject key: got %q, want %q", objectName, tc.wantKey)
					}
					return nil
				},
			}

			s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
			if err := s.Delete(context.Background(), tc.res, mac); err != nil {
				t.Fatalf("Delete() unexpected error: %v", err)
			}
		})
	}
}

func TestDelete_RemoveObjectError(t *testing.T) {
	t.Parallel()

	m := &mockMinioClient{
		removeObjectFn: func(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
			return errors.New("rm fail")
		},
	}

	s := &Store{minioClient: m, bucket: "mybucket", prefixDir: "/repo/"}
	err := s.Delete(context.Background(), connectorstorage.StorageResourceLock, makeTestMAC(0x70))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "remove lock object") {
		t.Fatalf("unexpected error: %v", err)
	}
}
