/* Package storage is Plakar's native-API storage connector for Backblaze B2.

This connector stores and retrieves the repository's own internal objects
(CONFIG, packfiles, states, locks) in a B2 bucket. Unlike the S3-compatible
variant, this implementation talks directly to Backblaze's native HTTP API
through `B2NativeClient`.
*/

package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"strings"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
)

// b2NativeAPI is the subset of native client operations that `NativeStore`
// depends on. Keeping this as an interface makes unit tests easy to mock
// without real network calls.
type b2NativeAPI interface {
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	MakeBucket(ctx context.Context, bucketName, bucketType string) (string, error)
	StatObject(ctx context.Context, bucketName, fileName string) (*B2FileInfo, error)
	PutObject(ctx context.Context, bucketName, fileName string, rd io.Reader, contentType string) (int64, error)
	GetObject(ctx context.Context, bucketName, fileName string, rg *storage.Range) (io.ReadCloser, error)
	ListObjects(ctx context.Context, bucketName, prefix string) ([]B2FileInfo, error)
	RemoveObject(ctx context.Context, bucketName, fileName string) error
}

// init registers this connector's constructor with Kloset's in-process
// storage backend registry (storage.New/storage.Open), matching the pattern
// used by every other storage connector in this repo (e.g. backblaze_s3,
// and the upstream s3/azblob integrations). The out-of-process plugin
// entrypoint (plugin/storage/main.go) invokes NewNativeStore directly and
// does not depend on this registration, but registering here keeps this
// package usable if it's ever imported directly (embedded/in-process build,
// or a future in-process test harness) instead of only via the plugin
// subprocess.
func init() {
	storage.Register("b2", 0, NewNativeStore)
}

// NativeStore keeps all state needed by the native connector across calls.
//
//   - `client` performs actual B2 API operations.
//   - `bucket` is the target B2 bucket.
//   - `prefixDir` is an in-bucket folder prefix, so multiple repositories can
//     safely share one bucket.
//   - `host` is preserved for metadata/reporting compatibility (`Origin()`).
type NativeStore struct {
	client    b2NativeAPI
	host      string
	bucket    string
	prefixDir string
}

// NewNativeStore validates connector config and constructs a ready-to-use
// native B2-backed store instance.
//
// Supported location forms:
//  1. b2://<bucket>/<optional-prefix>               (native preferred)
//  2. b2://<endpoint>/<bucket>/<optional-prefix>    (legacy compatible)
//
// This function only prepares state/client; repository existence checks happen
// later in Create/Open/Ping.
func NewNativeStore(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
	_ = ctx
	_ = proto

	accessKey, ok := storeConfig["access_key"]
	if !ok {
		debugf("NewNativeStore: missing access_key")
		return nil, fmt.Errorf("missing access_key")
	}
	secretAccessKey, ok := storeConfig["secret_access_key"]
	if !ok {
		debugf("NewNativeStore: missing secret_access_key")
		return nil, fmt.Errorf("missing secret_access_key")
	}

	u, err := url.Parse(storeConfig["location"])
	if err != nil {
		debugf("NewNativeStore: parse location failed: %v", err)
		return nil, fmt.Errorf("parse location: %w", err)
	}

	host := u.Host
	trimmedPath := strings.TrimPrefix(u.Path, "/")

	// Native mode doesn't need an endpoint, but we still accept endpoint-style
	// locations for backward compatibility.
	var bucket, prefixDir string
	if strings.Contains(host, ".") {
		bucket, prefixDir, _ = strings.Cut(trimmedPath, "/")
	} else {
		bucket = host
		prefixDir = trimmedPath
	}

	if bucket == "" {
		debugf("NewNativeStore: parsed empty bucket from location=%q host=%q path=%q", storeConfig["location"], host, trimmedPath)
		return nil, fmt.Errorf("failed to parse the location: bucket name is empty")
	}

	if !strings.HasPrefix(prefixDir, "/") {
		prefixDir = "/" + prefixDir
	}
	if !strings.HasSuffix(prefixDir, "/") {
		prefixDir += "/"
	}

	client := NewB2NativeClient(accessKey, secretAccessKey, B2ClientOptions{})
	debugf("NativeStore initialized bucket=%q prefix=%q host=%q", bucket, prefixDir, host)

	return &NativeStore{
		client:    client,
		host:      host,
		bucket:    bucket,
		prefixDir: prefixDir,
	}, nil
}

func (s *NativeStore) realpath(path string) string {
	return strings.TrimPrefix(s.prefixDir+path, "/")
}

// Create initializes a brand-new repository at this bucket/prefix.
//
// Behavior:
// - create bucket if missing,
// - fail if CONFIG already exists at this prefix,
// - otherwise write CONFIG.
func (s *NativeStore) Create(ctx context.Context, config []byte) error {
	debugf("Create: start bucket=%q key=%q", s.bucket, s.realpath("CONFIG"))
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		debugf("Create: bucket exists check failed bucket=%q err=%v", s.bucket, err)
		return fmt.Errorf("check if bucket exists: %w", err)
	}
	if !exists {
		if _, err := s.client.MakeBucket(ctx, s.bucket, "allPrivate"); err != nil {
			debugf("Create: make bucket failed bucket=%q err=%v", s.bucket, err)
			return fmt.Errorf("make bucket: %w", err)
		}
	}

	_, err = s.client.StatObject(ctx, s.bucket, s.realpath("CONFIG"))
	if err != nil {
		if !errors.Is(err, ErrB2FileNotFound) {
			debugf("Create: stat CONFIG failed bucket=%q key=%q err=%v", s.bucket, s.realpath("CONFIG"), err)
			return fmt.Errorf("stat object CONFIG: %w", err)
		}
	} else {
		debugf("Create: repository already initialized bucket=%q key=%q", s.bucket, s.realpath("CONFIG"))
		return fmt.Errorf("bucket already initialized")
	}

	_, err = s.client.PutObject(ctx, s.bucket, s.realpath("CONFIG"), bytes.NewReader(config), "application/octet-stream")
	if err != nil {
		debugf("Create: put CONFIG failed bucket=%q key=%q err=%v", s.bucket, s.realpath("CONFIG"), err)
		return fmt.Errorf("put object CONFIG: %w", err)
	}
	return nil
}

func (s *NativeStore) Open(ctx context.Context) ([]byte, error) {
	debugf("Open: start bucket=%q key=%q", s.bucket, s.realpath("CONFIG"))
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		debugf("Open: bucket exists check failed bucket=%q err=%v", s.bucket, err)
		return nil, fmt.Errorf("error checking if bucket exists: %w", err)
	}
	if !exists {
		debugf("Open: bucket does not exist bucket=%q", s.bucket)
		return nil, fmt.Errorf("bucket does not exist")
	}

	// GetObject returns a stream; defer ensures it is always closed on return,
	// preventing HTTP body leaks and allowing connection reuse.
	object, err := s.client.GetObject(ctx, s.bucket, s.realpath("CONFIG"), nil)
	if err != nil {
		// Normalize "CONFIG doesn't exist yet" to the standard fs.ErrNotExist
		// sentinel, matching the S3-compatible module's Open() behavior, so
		// callers can use errors.Is(err, fs.ErrNotExist) regardless of which
		// connector backs the repository.
		if errors.Is(err, ErrB2FileNotFound) {
			debugf("Open: CONFIG missing bucket=%q key=%q", s.bucket, s.realpath("CONFIG"))
			return nil, fs.ErrNotExist
		}
		debugf("Open: get CONFIG failed bucket=%q key=%q err=%v", s.bucket, s.realpath("CONFIG"), err)
		return nil, fmt.Errorf("error getting object: %w", err)
	}
	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		debugf("Open: read CONFIG failed bucket=%q key=%q err=%v", s.bucket, s.realpath("CONFIG"), err)
		return nil, fmt.Errorf("error reading object: %w", err)
	}

	return data, nil
}

// Ping checks reachability and bucket existence without reading CONFIG.
func (s *NativeStore) Ping(ctx context.Context) error {
	debugf("Ping: start bucket=%q", s.bucket)
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		debugf("Ping: bucket exists check failed bucket=%q err=%v", s.bucket, err)
		return err
	}
	if !ok {
		debugf("Ping: bucket does not exist bucket=%q", s.bucket)
		return fmt.Errorf("bucket does not exist")
	}
	return nil
}

func (s *NativeStore) Origin() string        { return s.host }
func (s *NativeStore) Root() string          { return path.Join("/", s.bucket, s.prefixDir) }
func (s *NativeStore) Type() string          { return "b2" }
func (s *NativeStore) Flags() location.Flags { return 0 }

// Mode reports read/write capabilities for this connector.
func (s *NativeStore) Mode(ctx context.Context) (storage.Mode, error) {
	return storage.ModeRead | storage.ModeWrite, nil
}

// Size returns -1 (unknown) to delegate aggregate sizing to repository state.
func (s *NativeStore) Size(ctx context.Context) (int64, error) {
	return -1, nil
}

// List enumerates MACs for a resource family by listing object keys under the
// corresponding prefix and decoding the MAC from each key.
func (s *NativeStore) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	var prefix string
	var prefixSize int

	switch res {
	case storage.StorageResourcePackfile:
		prefix = s.realpath("packfiles/")
		prefixSize = len(prefix) + 3
	case storage.StorageResourceState:
		prefix = s.realpath("states/")
		prefixSize = len(prefix) + 3
	case storage.StorageResourceLock:
		prefix = s.realpath("locks/")
		prefixSize = len(prefix)
	default:
		return nil, errors.ErrUnsupported
	}
	debugf("List: start bucket=%q prefix=%q res=%s", s.bucket, prefix, res)

	files, err := s.client.ListObjects(ctx, s.bucket, prefix)
	if err != nil {
		debugf("List: list objects failed bucket=%q prefix=%q res=%s err=%v", s.bucket, prefix, res, err)
		return nil, fmt.Errorf("list %s objects: %w", res, err)
	}

	ret := make([]objects.MAC, 0)
	for _, object := range files {
		if strings.HasPrefix(object.FileName, prefix) && len(object.FileName) >= prefixSize {
			t, err := hex.DecodeString(object.FileName[prefixSize:])
			if err != nil {
				debugf("List: decode key failed res=%s key=%q err=%v", res, object.FileName, err)
				return nil, fmt.Errorf("decode %s key: %w", res, err)
			}
			if len(t) != 32 {
				continue
			}
			ret = append(ret, objects.MAC(t))
		}
	}
	return ret, nil
}

// Put stores a single object identified by `mac` under its resource-specific
// key path.
func (s *NativeStore) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	var key string
	if res == storage.StorageResourcePackfile {
		key = s.realpath(fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac))
	} else if res == storage.StorageResourceState {
		key = s.realpath(fmt.Sprintf("states/%02x/%016x", mac[0], mac))
	} else if res == storage.StorageResourceLock {
		key = s.realpath(fmt.Sprintf("locks/%016x", mac))
	} else {
		return -1, errors.ErrUnsupported
	}
	debugf("Put: start bucket=%q key=%q res=%s", s.bucket, key, res)

	size, err := s.client.PutObject(ctx, s.bucket, key, rd, "application/octet-stream")
	if err != nil {
		debugf("Put: put object failed bucket=%q key=%q res=%s err=%v", s.bucket, key, res, err)
		return 0, fmt.Errorf("put %s object: %w", res, err)
	}
	return size, nil
}

// Get fetches a single object by resource + MAC.
func (s *NativeStore) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	var key string
	switch res {
	case storage.StorageResourcePackfile:
		key = s.realpath(fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac))
	case storage.StorageResourceState:
		key = s.realpath(fmt.Sprintf("states/%02x/%016x", mac[0], mac))
	case storage.StorageResourceLock:
		key = s.realpath(fmt.Sprintf("locks/%016x", mac))
	default:
		return nil, errors.ErrUnsupported
	}
	if rg != nil {
		debugf("Get: start bucket=%q key=%q res=%s range_offset=%d range_length=%d", s.bucket, key, res, rg.Offset, rg.Length)
	} else {
		debugf("Get: start bucket=%q key=%q res=%s", s.bucket, key, res)
	}

	object, err := s.client.GetObject(ctx, s.bucket, key, rg)
	if err != nil {
		debugf("Get: get object failed bucket=%q key=%q res=%s err=%v", s.bucket, key, res, err)
		return nil, fmt.Errorf("get %s object: %w", res, err)
	}

	return object, nil

}

// Delete removes one object by resource + MAC.
func (s *NativeStore) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	var key string
	switch res {
	case storage.StorageResourcePackfile:
		key = s.realpath(fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac))
	case storage.StorageResourceState:
		key = s.realpath(fmt.Sprintf("states/%02x/%016x", mac[0], mac))
	case storage.StorageResourceLock:
		key = s.realpath(fmt.Sprintf("locks/%016x", mac))
	default:
		return errors.ErrUnsupported
	}
	debugf("Delete: start bucket=%q key=%q res=%s", s.bucket, key, res)

	if err := s.client.RemoveObject(ctx, s.bucket, key); err != nil {
		debugf("Delete: remove object failed bucket=%q key=%q res=%s err=%v", s.bucket, key, res, err)
		return fmt.Errorf("remove %s object: %w", res, err)
	}
	return nil
}

// Close releases connector resources. NativeStore has no persistent handles,
// so this is currently a no-op.
func (s *NativeStore) Close(ctx context.Context) error {
	debugf("Close: start bucket=%q", s.bucket)
	return nil
}
