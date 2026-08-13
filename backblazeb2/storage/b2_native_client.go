package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/Backblaze/blazer/b2"
	"github.com/PlakarKorp/kloset/connectors/storage"
)

// ErrB2FileNotFound normalizes "not found" outcomes across the connector.
var ErrB2FileNotFound = errors.New("b2 file not found")

// B2FileInfo is the minimal metadata shape expected by NativeStore.
type B2FileInfo struct {
	FileID   string
	FileName string
	Action   string
	Size     int64
}

// b2Facade is the single dependency surface used by B2NativeClient.
// It keeps production wiring straightforward while remaining easy to mock.
type b2Facade interface {
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	MakeBucket(ctx context.Context, bucketName, bucketType string) (string, error)
	ObjectAttrs(ctx context.Context, bucketName, fileName string) (*b2.Attrs, error)
	ObjectUpload(ctx context.Context, bucketName, fileName string, rd io.Reader, contentType string) (int64, error)
	ObjectDownload(ctx context.Context, bucketName, fileName string) (io.ReadCloser, error)
	ObjectRangeDownload(ctx context.Context, bucketName, fileName string, offset int64, length int64) (io.ReadCloser, error)

	ObjectDelete(ctx context.Context, bucketName, fileName string) error
	ListPrefix(ctx context.Context, bucketName, prefix string) ([]*b2.Attrs, error)
}

type b2FacadeFactory func(ctx context.Context, keyID, appKey string) (b2Facade, error)

// B2ClientOptions allows dependency injection for tests.
type B2ClientOptions struct {
	FacadeFactory b2FacadeFactory
	IsNotExist    func(err error) bool
}

// B2NativeClient is the native client adapter used by NativeStore.
//
// This implementation delegates native B2 operations to blazer's `b2` package.
type B2NativeClient struct {
	keyID      string
	appKey     string
	factory    b2FacadeFactory
	isNotExist func(error) bool

	mu     sync.Mutex
	facade b2Facade
}

func defaultB2FacadeFactory(ctx context.Context, keyID, appKey string) (b2Facade, error) {
	client, err := b2.NewClient(ctx, keyID, appKey)
	if err != nil {
		return nil, err
	}
	return &blazerFacade{client: client}, nil
}

func defaultIsNotExist(err error) bool {
	return b2.IsNotExist(err)
}

// NewB2NativeClient creates a new native B2 client adapter.
func NewB2NativeClient(keyID, appKey string, opts B2ClientOptions) *B2NativeClient {
	factory := opts.FacadeFactory
	if factory == nil {
		factory = defaultB2FacadeFactory
	}
	isNotExist := opts.IsNotExist
	if isNotExist == nil {
		isNotExist = defaultIsNotExist
	}

	return &B2NativeClient{
		keyID:      keyID,
		appKey:     appKey,
		factory:    factory,
		isNotExist: isNotExist,
	}
}

func (c *B2NativeClient) ensureFacade(ctx context.Context) (b2Facade, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.facade != nil {
		return c.facade, nil
	}
	f, err := c.factory(ctx, c.keyID, c.appKey)
	if err != nil {
		debugf("ensureFacade: authorize account failed err=%v", err)
		return nil, fmt.Errorf("authorize account: %w", err)
	}
	c.facade = f
	return c.facade, nil
}

func mapObjectState(st b2.ObjectState) string {
	switch st {
	case b2.Uploaded:
		return "upload"
	case b2.Hider:
		return "hide"
	case b2.Started:
		return "start"
	default:
		return "unknown"
	}
}

func (c *B2NativeClient) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("BucketExists: ensure facade failed bucket=%q err=%v", bucketName, err)
		return false, err
	}
	ok, err := f.BucketExists(ctx, bucketName)
	if err != nil {
		if c.isNotExist(err) {
			debugf("BucketExists: bucket not found bucket=%q", bucketName)
			return false, nil
		}
		debugf("BucketExists: backend error bucket=%q err=%v", bucketName, err)
		return false, err
	}
	return ok, nil
}

func (c *B2NativeClient) MakeBucket(ctx context.Context, bucketName, bucketType string) (string, error) {
	if bucketType == "" {
		bucketType = "allPrivate"
	}
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("MakeBucket: ensure facade failed bucket=%q err=%v", bucketName, err)
		return "", err
	}
	id, err := f.MakeBucket(ctx, bucketName, bucketType)
	if err != nil {
		debugf("MakeBucket: backend create failed bucket=%q type=%q err=%v", bucketName, bucketType, err)
		return "", err
	}
	return id, nil
}

func (c *B2NativeClient) StatObject(ctx context.Context, bucketName, fileName string) (*B2FileInfo, error) {
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("StatObject: ensure facade failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return nil, err
	}

	attrs, err := f.ObjectAttrs(ctx, bucketName, fileName)
	if err != nil {
		if c.isNotExist(err) {
			debugf("StatObject: not found bucket=%q key=%q", bucketName, fileName)
			return nil, ErrB2FileNotFound
		}
		debugf("StatObject: backend error bucket=%q key=%q err=%v", bucketName, fileName, err)
		return nil, err
	}

	return &B2FileInfo{
		FileName: attrs.Name,
		Action:   mapObjectState(attrs.Status),
		Size:     attrs.Size,
	}, nil
}

func (c *B2NativeClient) PutObject(ctx context.Context, bucketName, fileName string, rd io.Reader, contentType string) (int64, error) {
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("PutObject: ensure facade failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return 0, err
	}
	n, err := f.ObjectUpload(ctx, bucketName, fileName, rd, contentType)
	if err != nil {
		debugf("PutObject: upload failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return 0, err
	}
	return n, nil
}

func (c *B2NativeClient) GetObject(ctx context.Context, bucketName, fileName string, rg *storage.Range) (io.ReadCloser, error) {
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("GetObject: ensure facade failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return nil, err
	}
	if _, err := f.ObjectAttrs(ctx, bucketName, fileName); err != nil {
		if c.isNotExist(err) {
			debugf("GetObject: not found bucket=%q key=%q", bucketName, fileName)
			return nil, ErrB2FileNotFound
		}
		debugf("GetObject: attrs failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return nil, err
	}
	if rg != nil {
		rc, err := f.ObjectRangeDownload(ctx, bucketName, fileName, int64(rg.Offset), int64(rg.Length))
		if err != nil {
			debugf("GetObject: range download failed bucket=%q key=%q offset=%d length=%d err=%v", bucketName, fileName, rg.Offset, rg.Length, err)
			return nil, err
		}
		return rc, nil
	}
	rc, err := f.ObjectDownload(ctx, bucketName, fileName)
	if err != nil {
		debugf("GetObject: download failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return nil, err
	}
	return rc, nil
}

func (c *B2NativeClient) ListObjects(ctx context.Context, bucketName, prefix string) ([]B2FileInfo, error) {
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("ListObjects: ensure facade failed bucket=%q prefix=%q err=%v", bucketName, prefix, err)
		return nil, err
	}

	attrsList, err := f.ListPrefix(ctx, bucketName, prefix)
	if err != nil {
		debugf("ListObjects: list prefix failed bucket=%q prefix=%q err=%v", bucketName, prefix, err)
		return nil, err
	}

	files := make([]B2FileInfo, 0, len(attrsList))
	for _, attrs := range attrsList {
		if attrs == nil {
			continue
		}
		if !strings.HasPrefix(attrs.Name, prefix) {
			continue
		}
		if attrs.Status == b2.Hider {
			continue
		}
		files = append(files, B2FileInfo{
			FileName: attrs.Name,
			Action:   mapObjectState(attrs.Status),
			Size:     attrs.Size,
		})
	}
	return files, nil
}

func (c *B2NativeClient) RemoveObject(ctx context.Context, bucketName, fileName string) error {
	f, err := c.ensureFacade(ctx)
	if err != nil {
		debugf("RemoveObject: ensure facade failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return err
	}
	if err := f.ObjectDelete(ctx, bucketName, fileName); err != nil {
		if c.isNotExist(err) {
			debugf("RemoveObject: not found bucket=%q key=%q", bucketName, fileName)
			return ErrB2FileNotFound
		}
		debugf("RemoveObject: delete failed bucket=%q key=%q err=%v", bucketName, fileName, err)
		return err
	}
	return nil
}

// blazerFacade caches a single resolved *b2.Bucket handle. In practice a
// NativeStore is bound to exactly one bucket for its whole lifetime (the
// bucket name comes from the repository's location and never changes), so a
// single cached (name, handle) pair is enough - no need for a general-purpose
// map. A plain Mutex (not sync.Once) is used deliberately: if client.Bucket
// fails (e.g. a transient network error), we want the *next* call to retry
// rather than permanently remember the failure.
type blazerFacade struct {
	client *b2.Client

	mu           sync.Mutex
	bucketName   string
	bucketHandle *b2.Bucket
}

func bucketTypeFromString(bucketType string) b2.BucketType {
	switch bucketType {
	case "allPublic":
		return b2.Public
	case "snapshot":
		return b2.Snapshot
	default:
		return b2.Private
	}
}

func (f *blazerFacade) bucket(ctx context.Context, bucketName string) (*b2.Bucket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.bucketHandle != nil && f.bucketName == bucketName {
		return f.bucketHandle, nil
	}
	b, err := f.client.Bucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	f.bucketName = bucketName
	f.bucketHandle = b
	return b, nil
}

func (f *blazerFacade) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	_, err := f.bucket(ctx, bucketName)
	if err != nil {
		if b2.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *blazerFacade) MakeBucket(ctx context.Context, bucketName, bucketType string) (string, error) {
	b, err := f.client.NewBucket(ctx, bucketName, &b2.BucketAttrs{Type: bucketTypeFromString(bucketType)})
	if err != nil {
		return "", err
	}

	f.mu.Lock()
	f.bucketName = bucketName
	f.bucketHandle = b
	f.mu.Unlock()

	return bucketName, nil
}

func (f *blazerFacade) ObjectAttrs(ctx context.Context, bucketName, fileName string) (*b2.Attrs, error) {
	bucket, err := f.bucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return bucket.Object(fileName).Attrs(ctx)
}

func (f *blazerFacade) ObjectUpload(ctx context.Context, bucketName, fileName string, rd io.Reader, contentType string) (int64, error) {
	bucket, err := f.bucket(ctx, bucketName)
	if err != nil {
		return 0, err
	}

	attrs := &b2.Attrs{ContentType: contentType}
	if attrs.ContentType == "" {
		attrs.ContentType = "application/octet-stream"
	}
	w := bucket.Object(fileName).NewWriter(ctx, b2.WithAttrsOption(attrs))
	n, err := io.Copy(w, rd)
	if err != nil {
		_ = w.Close()
		return n, err
	}
	if err := w.Close(); err != nil {
		return n, err
	}
	return n, nil
}

func (f *blazerFacade) ObjectDownload(ctx context.Context, bucketName, fileName string) (io.ReadCloser, error) {
	bucket, err := f.bucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return bucket.Object(fileName).NewReader(ctx), nil
}

func (f *blazerFacade) ObjectRangeDownload(ctx context.Context, bucketName, fileName string, offset int64, length int64) (io.ReadCloser, error) {
	bucket, err := f.bucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	// return bucket.Object(fileName).NewReader(ctx), nil
	return bucket.Object(fileName).NewRangeReader(ctx, offset, length), nil
}

func (f *blazerFacade) ObjectDelete(ctx context.Context, bucketName, fileName string) error {
	bucket, err := f.bucket(ctx, bucketName)
	if err != nil {
		return err
	}
	return bucket.Object(fileName).Delete(ctx)
}

func (f *blazerFacade) ListPrefix(ctx context.Context, bucketName, prefix string) ([]*b2.Attrs, error) {
	bucket, err := f.bucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	// Do not call Object.Attrs() per entry; it may trigger extra metadata fetches.
	// list_file_names already gives us current (non-hidden) objects by prefix.
	iter := bucket.List(ctx, b2.ListPrefix(prefix))
	out := make([]*b2.Attrs, 0)
	for iter.Next() {
		obj := iter.Object()
		out = append(out, &b2.Attrs{
			Name:   obj.Name(),
			Status: b2.Uploaded,
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
