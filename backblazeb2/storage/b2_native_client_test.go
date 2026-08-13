package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/Backblaze/blazer/b2"
	connectorstorage "github.com/PlakarKorp/kloset/connectors/storage"
)

var errNotFound = errors.New("not found")

type mockFacade struct {
	buckets              map[string]map[string]*mockObject
	newBucketInvoked     bool
	newBucketType        string
	downloadInvoked      bool
	rangeDownloadInvoked bool
	lastRangeOffset      int64
	lastRangeLength      int64
}

type mockObject struct {
	name      string
	data      []byte
	status    b2.ObjectState
	attrsErr  error
	readErr   error
	deleteErr error
	deleted   bool
}

func (m *mockFacade) ensureObject(bucketName, fileName string) *mockObject {
	bucket, ok := m.buckets[bucketName]
	if !ok {
		return &mockObject{name: fileName, attrsErr: errNotFound, readErr: errNotFound, deleteErr: errNotFound}
	}
	if o, ok := bucket[fileName]; ok {
		return o
	}
	o := &mockObject{name: fileName, attrsErr: errNotFound, readErr: errNotFound, deleteErr: errNotFound}
	bucket[fileName] = o
	return o
}

func (m *mockFacade) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	_, ok := m.buckets[bucketName]
	return ok, nil
}

func (m *mockFacade) MakeBucket(ctx context.Context, bucketName, bucketType string) (string, error) {
	m.newBucketInvoked = true
	m.newBucketType = bucketType
	if m.buckets == nil {
		m.buckets = map[string]map[string]*mockObject{}
	}
	m.buckets[bucketName] = map[string]*mockObject{}
	return bucketName, nil
}

func (m *mockFacade) ObjectAttrs(ctx context.Context, bucketName, fileName string) (*b2.Attrs, error) {
	o := m.ensureObject(bucketName, fileName)
	if o.attrsErr != nil {
		return nil, o.attrsErr
	}
	return &b2.Attrs{Name: o.name, Size: int64(len(o.data)), Status: o.status}, nil
}

func (m *mockFacade) ObjectUpload(ctx context.Context, bucketName, fileName string, rd io.Reader, contentType string) (int64, error) {
	o := m.ensureObject(bucketName, fileName)
	p, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}
	o.data = slices.Clone(p)
	o.attrsErr = nil
	o.readErr = nil
	o.deleteErr = nil
	if o.status == 0 {
		o.status = b2.Uploaded
	}
	return int64(len(p)), nil
}

func (m *mockFacade) ObjectDownload(ctx context.Context, bucketName, fileName string) (io.ReadCloser, error) {
	m.downloadInvoked = true
	o := m.ensureObject(bucketName, fileName)
	if o.readErr != nil {
		return nil, o.readErr
	}
	return io.NopCloser(strings.NewReader(string(o.data))), nil
}

func (m *mockFacade) ObjectRangeDownload(ctx context.Context, bucketName, fileName string, offset int64, length int64) (io.ReadCloser, error) {
	m.rangeDownloadInvoked = true
	m.lastRangeOffset = offset
	m.lastRangeLength = length
	o := m.ensureObject(bucketName, fileName)
	if o.readErr != nil {
		return nil, o.readErr
	}

	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range: offset=%d length=%d", offset, length)
	}

	if offset >= int64(len(o.data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	end := offset + length
	if end > int64(len(o.data)) {
		end = int64(len(o.data))
	}

	return io.NopCloser(strings.NewReader(string(o.data[offset:end]))), nil
}

func (m *mockFacade) ObjectDelete(ctx context.Context, bucketName, fileName string) error {
	o := m.ensureObject(bucketName, fileName)
	if o.deleteErr != nil {
		return o.deleteErr
	}
	o.deleted = true
	return nil
}

func (m *mockFacade) ListPrefix(ctx context.Context, bucketName, prefix string) ([]*b2.Attrs, error) {
	bucket, ok := m.buckets[bucketName]
	if !ok {
		return nil, errNotFound
	}
	attrs := make([]*b2.Attrs, 0)
	for _, o := range bucket {
		if strings.HasPrefix(o.name, prefix) {
			attrs = append(attrs, &b2.Attrs{Name: o.name, Size: int64(len(o.data)), Status: o.status})
		}
	}
	return attrs, nil
}

// TestB2NativeClient_UsesFactoryOnceAndBucketExists verifies lazy facade
// initialization is reused and bucket existence checks map missing bucket to
// (false, nil).
func TestB2NativeClient_UsesFactoryOnceAndBucketExists(t *testing.T) {
	t.Parallel()

	factoryCalls := 0
	mf := &mockFacade{buckets: map[string]map[string]*mockObject{
		"repo": {},
	}}

	c := NewB2NativeClient("id", "key", B2ClientOptions{
		FacadeFactory: func(ctx context.Context, keyID, appKey string) (b2Facade, error) {
			factoryCalls++
			return mf, nil
		},
		IsNotExist: func(err error) bool { return errors.Is(err, errNotFound) },
	})

	ok, err := c.BucketExists(context.Background(), "repo")
	if err != nil || !ok {
		t.Fatalf("BucketExists(repo) = (%v, %v), want (true, nil)", ok, err)
	}
	ok, err = c.BucketExists(context.Background(), "missing")
	if err != nil || ok {
		t.Fatalf("BucketExists(missing) = (%v, %v), want (false, nil)", ok, err)
	}
	if factoryCalls != 1 {
		t.Fatalf("factory calls: got %d, want 1", factoryCalls)
	}
}

// TestB2NativeClient_MakeBucketAndObjectFlow validates the happy-path flow for
// make bucket, put, stat, get, list, and remove operations.
func TestB2NativeClient_MakeBucketAndObjectFlow(t *testing.T) {
	t.Parallel()

	mf := &mockFacade{buckets: map[string]map[string]*mockObject{}}
	c := NewB2NativeClient("id", "key", B2ClientOptions{
		FacadeFactory: func(ctx context.Context, keyID, appKey string) (b2Facade, error) {
			return mf, nil
		},
		IsNotExist: func(err error) bool { return errors.Is(err, errNotFound) },
	})

	id, err := c.MakeBucket(context.Background(), "repo", "allPrivate")
	if err != nil {
		t.Fatalf("MakeBucket() unexpected error: %v", err)
	}
	if id != "repo" {
		t.Fatalf("MakeBucket() id: got %q, want %q", id, "repo")
	}
	if !mf.newBucketInvoked || mf.newBucketType != "allPrivate" {
		t.Fatalf("NewBucket not invoked as expected")
	}

	n, err := c.PutObject(context.Background(), "repo", "locks/a", strings.NewReader("data"), "application/octet-stream")
	if err != nil || n != 4 {
		t.Fatalf("PutObject() = (%d, %v), want (4, nil)", n, err)
	}

	info, err := c.StatObject(context.Background(), "repo", "locks/a")
	if err != nil {
		t.Fatalf("StatObject() unexpected error: %v", err)
	}
	if info.FileName != "locks/a" || info.Action != "upload" || info.Size != 4 {
		t.Fatalf("StatObject() unexpected info: %#v", info)
	}

	rc, err := c.GetObject(context.Background(), "repo", "locks/a", nil)
	if err != nil {
		t.Fatalf("GetObject() unexpected error: %v", err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "data" {
		t.Fatalf("GetObject() data: got %q, want %q", string(data), "data")
	}

	list, err := c.ListObjects(context.Background(), "repo", "locks/")
	if err != nil {
		t.Fatalf("ListObjects() unexpected error: %v", err)
	}
	if len(list) != 1 || list[0].FileName != "locks/a" {
		t.Fatalf("ListObjects() unexpected list: %#v", list)
	}

	if err := c.RemoveObject(context.Background(), "repo", "locks/a"); err != nil {
		t.Fatalf("RemoveObject() unexpected error: %v", err)
	}
}

// TestB2NativeClient_NotFoundMapping ensures backend not-found errors are
// normalized to ErrB2FileNotFound for stat/get/remove.
func TestB2NativeClient_NotFoundMapping(t *testing.T) {
	t.Parallel()

	mf := &mockFacade{buckets: map[string]map[string]*mockObject{
		"repo": {},
	}}
	c := NewB2NativeClient("id", "key", B2ClientOptions{
		FacadeFactory: func(ctx context.Context, keyID, appKey string) (b2Facade, error) {
			return mf, nil
		},
		IsNotExist: func(err error) bool { return errors.Is(err, errNotFound) },
	})

	if _, err := c.StatObject(context.Background(), "repo", "missing"); !errors.Is(err, ErrB2FileNotFound) {
		t.Fatalf("StatObject missing: got %v, want %v", err, ErrB2FileNotFound)
	}
	if _, err := c.GetObject(context.Background(), "repo", "missing", nil); !errors.Is(err, ErrB2FileNotFound) {
		t.Fatalf("GetObject missing: got %v, want %v", err, ErrB2FileNotFound)
	}
	if err := c.RemoveObject(context.Background(), "repo", "missing"); !errors.Is(err, ErrB2FileNotFound) {
		t.Fatalf("RemoveObject missing: got %v, want %v", err, ErrB2FileNotFound)
	}
}

// TestB2NativeClient_GetObject_WithRange verifies range requests are forwarded
// to ObjectRangeDownload with correct offsets/length and without full download.
func TestB2NativeClient_GetObject_WithRange(t *testing.T) {
	t.Parallel()

	mf := &mockFacade{buckets: map[string]map[string]*mockObject{
		"repo": {
			"locks/a": {
				name:      "locks/a",
				data:      []byte("0123456789"),
				status:    b2.Uploaded,
				attrsErr:  nil,
				readErr:   nil,
				deleteErr: nil,
			},
		},
	}}

	c := NewB2NativeClient("id", "key", B2ClientOptions{
		FacadeFactory: func(ctx context.Context, keyID, appKey string) (b2Facade, error) {
			return mf, nil
		},
		IsNotExist: func(err error) bool { return errors.Is(err, errNotFound) },
	})

	rc, err := c.GetObject(context.Background(), "repo", "locks/a", &connectorstorage.Range{Offset: 2, Length: 4})
	if err != nil {
		t.Fatalf("GetObject(range) unexpected error: %v", err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if string(data) != "2345" {
		t.Fatalf("GetObject(range) data: got %q, want %q", string(data), "2345")
	}
	if !mf.rangeDownloadInvoked {
		t.Fatalf("expected ObjectRangeDownload to be invoked")
	}
	if mf.lastRangeOffset != 2 || mf.lastRangeLength != 4 {
		t.Fatalf("range args mismatch: got offset=%d length=%d, want offset=2 length=4", mf.lastRangeOffset, mf.lastRangeLength)
	}
	if mf.downloadInvoked {
		t.Fatalf("expected full ObjectDownload not to be invoked for range request")
	}
}

// TestMapObjectState checks conversion from blazer object states to B2 action
// strings expected by the connector.
func TestMapObjectState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		st   b2.ObjectState
		want string
	}{
		{st: b2.Uploaded, want: "upload"},
		{st: b2.Hider, want: "hide"},
		{st: b2.Started, want: "start"},
		{st: b2.Unknown, want: "unknown"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d", tc.st), func(t *testing.T) {
			if got := mapObjectState(tc.st); got != tc.want {
				t.Fatalf("mapObjectState(%v): got %q, want %q", tc.st, got, tc.want)
			}
		})
	}
}

// TestDefaultIsNotExist_FalseForGenericError validates that non-b2 errors are
// not treated as not-found by the default helper.
func TestDefaultIsNotExist_FalseForGenericError(t *testing.T) {
	t.Parallel()

	if got := defaultIsNotExist(errors.New("boom")); got {
		t.Fatalf("defaultIsNotExist(generic error): got true, want false")
	}
}

// TestDefaultB2FacadeFactory_ContextCanceled verifies default factory surfaces
// errors when client construction context is already canceled.
func TestDefaultB2FacadeFactory_ContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f, err := defaultB2FacadeFactory(ctx, "invalid-key-id", "invalid-app-key")
	if err == nil {
		t.Fatalf("defaultB2FacadeFactory() expected error with canceled context, got facade=%#v", f)
	}
}

// TestBucketTypeFromString verifies bucket type string to blazer enum mapping,
// including fallback to private for unknown values.
func TestBucketTypeFromString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want b2.BucketType
	}{
		{in: "allPublic", want: b2.Public},
		{in: "snapshot", want: b2.Snapshot},
		{in: "allPrivate", want: b2.Private},
		{in: "", want: b2.Private},
		{in: "unknown", want: b2.Private},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			if got := bucketTypeFromString(tc.in); got != tc.want {
				t.Fatalf("bucketTypeFromString(%q): got %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
