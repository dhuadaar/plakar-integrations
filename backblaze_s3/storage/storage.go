/* Package storage is Plakar's "storage connector" for Backblaze B2.

A storage connector is the piece of code responsible for reading and
writing the repository's own internal files (not your backed-up files
directly - those go through a separate importer/exporter). Plakar calls
into this code whenever it needs to save or load a chunk of backup data.
This particular connector talks to Backblaze B2 using B2's S3-compatible
API, via the popular "minio-go" S3 client library.
*/

package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/reading"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

// minioClientAPI is the subset of minio client operations this connector
// uses. Keeping this as an interface allows unit tests to inject a mock
// client (no network required), while production still uses the real client.
type minioClientAPI interface {
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	MakeBucket(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	GetObjectReader(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
}

type realMinioClient struct {
	client *minio.Client
}

func (m *realMinioClient) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return m.client.BucketExists(ctx, bucketName)
}

func (m *realMinioClient) MakeBucket(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error {
	return m.client.MakeBucket(ctx, bucketName, opts)
}

func (m *realMinioClient) StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return m.client.StatObject(ctx, bucketName, objectName, opts)
}

func (m *realMinioClient) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
}

func (m *realMinioClient) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}

func (m *realMinioClient) GetObjectReader(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}

func (m *realMinioClient) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return m.client.ListObjects(ctx, bucketName, opts)
}

func (m *realMinioClient) RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error {
	return m.client.RemoveObject(ctx, bucketName, objectName, opts)
}

// Store holds everything this connector needs to remember between calls:
// the connection to B2, and which bucket/folder it's supposed to use.
type Store struct {
	minioClient minioClientAPI     // the actual S3-compatible client connected to B2
	host        string             // the B2 endpoint hostname, e.g. s3.us-west-004.backblazeb2.com
	bucket      string             // which B2 bucket the repository lives in
	prefixDir   string             // a "folder path" inside the bucket, so multiple repositories can share one bucket
	ssec        encrypt.ServerSide // optional customer-provided encryption key (SSE-C), if configured

	// A pool of reusable byte buffers, used only when uploading packfiles
	// (see Put below). Reusing buffers instead of allocating a new one for
	// every upload reduces memory churn under heavy backup load. This is a
	// pure performance optimization - functionally, it changes nothing.
	bufPool sync.Pool

	// Default options passed to every "upload a file" call to B2. Computed
	// once in NewStore and reused, rather than rebuilt on every upload.
	putObjectOptions minio.PutObjectOptions
}

/*
init runs automatically once, when this package is first loaded - we
never call it yourself. It tells Plakar's core "here is a storage backend
named 'b2s3'; if a repository location starts with 'b2s3://', hand it to
NewStore below to construct it." The `0` means this connector doesn't
need any of Plakar's special location flags (e.g. it isn't tied to the
local filesystem).

The protocol is named 'b2s3' (not 'b2') specifically so this S3-compatible
module can be installed side by side with the native backblazeb2 module
(which registers 'b2') without Plakar's location registry rejecting the
second `Register` call for an already-claimed scheme.
*/
func init() {
	storage.Register("b2s3", 0, NewStore)
}

/*
NewStore is called by Plakar once, when you first open a repository whose
location starts with "b2s3://". Its job is purely "read the configuration
the user provided and turn it into a ready-to-use B2 connection" - it
doesn't talk to B2 over the network yet at the end of this function
(aside from setting up the client), it just prepares everything so the
methods further down in this file (Create, Open, Put, Get, ...) can work.

storeConfig is a simple key/value map built from the location URL plus
any `key=value` options you passed on the command line (e.g. via
`plakar store add name b2s3://... access_key=... secret_access_key=...`).
*/
func NewStore(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
	// --- required credentials -------------------------------------------------
	// storeConfig["access_key"] looks up the value for that key; the second
	// return value (ok) tells you whether the key was present at all, which
	// is how Go distinguishes "missing" from "present but empty".
	var accessKey string
	if value, ok := storeConfig["access_key"]; !ok {
		return nil, fmt.Errorf("missing access_key")
	} else {
		accessKey = value
	}

	var secretAccessKey string
	if value, ok := storeConfig["secret_access_key"]; !ok {
		return nil, fmt.Errorf("missing secret_access_key")
	} else {
		secretAccessKey = value
	}

	// Optional server-side encryption with a customer-supplied key (SSE-C).
	// If set, B2 encrypts/decrypts the data using this key, and every
	// request we make later must present the same key again or B2 will
	// refuse to hand the data back.
	var ssec encrypt.ServerSide
	if value, ok := storeConfig["sse_customer_key"]; ok && value != "" {
		keyBytes, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid sse_customer_key: must be base64-encoded: %w", err)
		}
		ssec, err = encrypt.NewSSEC(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid sse_customer_key: %w", err)
		}
	}

	// --- figure out the B2 host, bucket name, and in-bucket folder -----------
	// This all comes from parsing the "location" URL, e.g.
	//   b2s3://s3.us-west-004.backblazeb2.com/mybucket/some/folder
	// url.Parse splits that into a host part (s3.us-west-004.backblazeb2.com)
	// and a path part (/mybucket/some/folder).
	u, err := url.Parse(storeConfig["location"])
	if err != nil {
		return nil, fmt.Errorf("parse location: %w", err)
	}

	host := u.Host
	// Path style only: b2s3://<endpoint>/<bucket>/<optional-prefix>
	// strings.Cut splits "mybucket/some/folder" into "mybucket" and
	// "some/folder" at the first "/". The trailing "_" throws away the
	// third return value (whether a separator was found at all) since
	// we don't need it here - if there's no "/", prefixDir is just "".
	bucket, prefixDir, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")

	if bucket == "" || host == "" {
		return nil, fmt.Errorf("failed to parse the location: bucket name or host name are empty")
	}

	// Normalize prefixDir to always look like "/some/folder/" (leading and
	// trailing slash), so later string concatenation in realpath() never
	// has to special-case a missing slash.
	if !strings.HasPrefix(prefixDir, "/") {
		prefixDir = "/" + prefixDir
	}

	if !strings.HasSuffix(prefixDir, "/") {
		prefixDir += "/"
	}

	// --- set up the actual HTTP/S3 client used to talk to B2 -----------------

	transport, err := minio.DefaultTransport(true)
	if err != nil {
		return nil, fmt.Errorf("failed to create default transport: %w", err)
	}

	// Initialize minio client object. "credentials.NewStaticV4" means "sign
	// every request using this fixed access key / secret key pair, using
	// AWS Signature Version 4" - the same signing scheme B2's S3-compatible
	// API requires.
	client, err := minio.New(host, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretAccessKey, ""),
		Secure:    true,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	client.SetAppInfo("plakar", "v1.1.0")

	// `&Store{...}` builds a new Store value and returns a pointer to it
	// (a reference to that one value in memory), which is what all the
	// methods below expect to receive as their `s` argument.
	return &Store{
		minioClient: &realMinioClient{client: client},
		host:        host,
		bucket:      bucket,
		prefixDir:   prefixDir,
		ssec:        ssec,

		bufPool: sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		},

		putObjectOptions: minio.PutObjectOptions{
			// Backblaze B2 returns the error "Unsupported header
			// 'x-amz-checksum-algorithm'" if SendContentMd5 is not set.
			SendContentMd5:       true,
			ServerSideEncryption: ssec,
		},
	}, nil
}

// realpath turns a "logical" path inside the repository (e.g. "CONFIG", or
// "packfiles/1a/xxxx...") into the actual object key we store in the B2
// bucket, by sticking the configured folder prefix in front of it. The
// leading "/" is stripped because S3/B2 object keys don't start with one.
//
// Note: this is a plugin-internal helper, not part of Plakar's storage
// connector interface. The SDK/core never calls realpath() directly; only
// this connector's own methods (Create/Open/List/Put/Get/Delete) use it.
//
// `(s *Store)` before the function name is what makes this a *method* on
// Store: it's a regular function that additionally receives the Store it's
// being called on as `s`, the same way `store.realpath("CONFIG")` works
// like `obj.realpath("CONFIG")` would in an object-oriented language.
func (s *Store) realpath(path string) string {
	return strings.TrimPrefix(s.prefixDir+path, "/")
}

// Create initializes a brand-new, empty repository.
//
// Behavior details:
//   - If the bucket does not exist yet, Create will create it.
//   - If a repository is already initialized at this configured prefix
//     (i.e. `CONFIG` already exists), Create fails with "bucket already initialized".
//   - If the bucket exists but this prefix is not initialized yet, Create writes
//     the initial `CONFIG` object and initializes a new repository at that prefix.
//
// So, to access/modify an existing Plakar repository, use Open (and then
// normal read/write operations), not Create.
//
// `config` is the raw bytes Plakar wants stored as that CONFIG object - this
// connector doesn't need to understand its contents, only to store and later
// hand them back as-is.
func (s *Store) Create(ctx context.Context, config []byte) error {
	exists, err := s.minioClient.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("check if bucket exists: %w", err)
	}
	if !exists {
		err = s.minioClient.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("make bucket: %w", err)
		}
	}

	// StatObject is like asking "does this file exist, and if so how big is
	// it / when was it modified?" without downloading it. Here we only care
	// about the "does it exist" part, to refuse to overwrite an existing
	// repository.
	_, err = s.minioClient.StatObject(ctx, s.bucket, s.realpath("CONFIG"), minio.StatObjectOptions{ServerSideEncryption: s.ssec})
	if err != nil {
		// "NoSuchKey" is the expected, non-error outcome here: it means
		// nothing is stored at this location yet, so we're clear to
		// initialize it. Any other error (network issue, permissions,
		// etc.) is a real problem and gets returned.
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			return fmt.Errorf("stat object CONFIG: %w", err)
		}
	} else {
		return fmt.Errorf("bucket already initialized")
	}

	_, err = s.minioClient.PutObject(ctx, s.bucket, s.realpath("CONFIG"), bytes.NewReader(config), int64(len(config)), s.putObjectOptions)
	if err != nil {
		return fmt.Errorf("put object CONFIG: %w", err)
	}

	return nil
}

// Open loads an existing repository's CONFIG object (the counterpart to
// Create), so Plakar can read back the settings it stored when the
// repository was first created.
func (s *Store) Open(ctx context.Context) ([]byte, error) {
	exists, err := s.minioClient.BucketExists(ctx, s.bucket)
	if err != nil {
		return nil, fmt.Errorf("error checking if bucket exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket does not exist")
	}

	// GetObject returns a streaming handle to the object's contents rather
	// than the full contents immediately - `defer object.Close()` schedules
	// closing that stream to happen automatically when this function
	// returns, however it returns (success or error), so we never forget to
	// release it. This avoids leaking the underlying HTTP response/body and
	// allows the client transport to reuse the connection.
	object, err := s.minioClient.GetObjectReader(ctx, s.bucket, s.realpath("CONFIG"), minio.GetObjectOptions{ServerSideEncryption: s.ssec})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("error getting object: %w", err)
	}
	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("error reading object: %w", err)
	}

	return data, nil
}

// Ping is a lightweight "are we actually able to reach this bucket right
// now" check, used e.g. by `plakar store ping`. It deliberately does less
// work than Open (no CONFIG object involved) since all it needs to confirm
// is connectivity + bucket existence.
func (s *Store) Ping(ctx context.Context) error {
	ok, err := s.minioClient.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("bucket does not exist")
	}
	return nil
}

// The four methods below are simple metadata getters Plakar uses to
// describe this repository (e.g. in logs or `plakar info`). Writing a
// method body on the same line as its signature, like `{ return s.host }`,
// is just a Go style choice for very short functions - it behaves exactly
// like a normal multi-line function.
func (s *Store) Origin() string        { return s.host }                                // which B2 endpoint this repository is on
func (s *Store) Root() string          { return path.Join("/", s.bucket, s.prefixDir) } // bucket + in-bucket folder, as a path
func (s *Store) Type() string          { return "b2s3" }                                // the connector's protocol name
func (s *Store) Flags() location.Flags { return 0 }                                     // no special location flags needed

// Mode reports whether this repository can be read from, written to, or
// both. Some storage backends (e.g. write-only archival tiers on other
// providers) only support one direction; B2 always supports both, so this
// is a constant.
func (s *Store) Mode(ctx context.Context) (storage.Mode, error) {
	return storage.ModeRead | storage.ModeWrite, nil
}

// Size would normally report the total size of the repository, but
// computing that accurately for an object-storage backend would mean
// listing and summing every single object, which is expensive. Returning
// -1 tells Plakar "not known - please figure it out yourself from the
// repository's own bookkeeping (states) instead of asking me directly."
func (s *Store) Size(ctx context.Context) (int64, error) {
	return -1, nil
}

// --- the three kinds of things actually stored in a repository -----------
//
// A Plakar repository stores three kinds of objects, each identified by a
// MAC (a 32-byte content hash/identifier, think of it like a checksum used
// as a filename):
//   - packfiles: the actual chunks of backed-up file data
//   - states:    bookkeeping/index data describing what's in the repository
//   - locks:     short-lived markers used to coordinate concurrent access
//
// List/Put/Get/Delete below all branch on `res` (which of the three kinds)
// to decide which "folder" inside the bucket to use, and how to turn a MAC
// into an object key.

// List returns every MAC currently stored for a given resource kind, by
// asking B2 to list every object under the matching folder and decoding
// each object's key back into a MAC.
func (s *Store) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	var prefix string
	var prefixSize int

	switch res {
	case storage.StorageResourcePackfile:
		// Packfiles and states are stored as e.g. "packfiles/1a/1a2b3c...":
		// the first byte of the MAC, written as two hex digits, is used as
		// a sub-folder (see Put below for why), so we need to skip past
		// "packfiles/" *and* that two-hex-digit sub-folder plus its slash
		// (hence "+ 3") before we reach the actual hex-encoded MAC.
		prefix = s.realpath("packfiles/")
		prefixSize = len(prefix) + 3 // prefix + len(%02x/) encoded
	case storage.StorageResourceState:
		prefix = s.realpath("states/")
		prefixSize = len(prefix) + 3 // prefix + len(%02x/) encoded
	case storage.StorageResourceLock:
		// Locks aren't split into sub-folders, so there's no extra offset
		// beyond the "locks/" prefix itself.
		prefix = s.realpath("locks/")
		prefixSize = len(prefix)
	default:
		return nil, errors.ErrUnsupported
	}

	ret := make([]objects.MAC, 0)
	// ListObjects gives back a channel we can range over, yielding one
	// object at a time as B2 returns them (rather than loading the whole
	// list into memory up front). Recursive: true means "include objects
	// nested under sub-folders too", which we need since packfiles/states
	// are split into those two-hex-digit sub-folders.
	for object := range s.minioClient.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, prefix) && len(object.Key) >= prefixSize {
			// Everything after the prefix (and sub-folder, if any) is the
			// hex-encoded MAC; decode it back into raw bytes.
			t, err := hex.DecodeString(object.Key[prefixSize:])
			if err != nil {
				return nil, fmt.Errorf("decode %s key: %w", res, err)
			}
			if len(t) != 32 {
				// Not a 32-byte MAC - skip it. This guards against
				// unrelated objects that might happen to sit under the
				// same prefix (e.g. leftover files from something else).
				continue
			}
			ret = append(ret, objects.MAC(t))
		}
	}
	return ret, nil
}

// Put uploads one object (a packfile, a state, or a lock) identified by its
// MAC, reading its content from rd (an io.Reader - anything that can be
// read from as a stream of bytes, without needing to know what's on the
// other end).
func (s *Store) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	switch res {
	case storage.StorageResourcePackfile:
		// Packfiles need to be uploaded with a known size up front (B2's
		// API wants a Content-Length), but `rd` doesn't necessarily know
		// its own size in advance. So we first copy its entire content
		// into an in-memory buffer, which does tell us its length, then
		// upload from that buffer instead. Buffers are borrowed from
		// bufPool (see the Store struct above) and returned afterwards, to
		// avoid allocating a fresh one on every single upload.
		buf := s.bufPool.Get().(*bytes.Buffer)
		copied, err := io.Copy(buf, rd)
		if err != nil {
			return 0, fmt.Errorf("read %s object: %w", res, err)
		}

		// The object key spreads packfiles across 256 sub-folders (one per
		// possible first-byte value, %02x = two hex digits), purely so that
		// no single folder ends up with an enormous number of objects in
		// it.
		info, err := s.minioClient.PutObject(ctx, s.bucket, s.realpath(fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac)), buf, copied, s.putObjectOptions)
		if err != nil {
			return 0, fmt.Errorf("put %s object: %w", res, err)
		}

		buf.Reset()
		s.bufPool.Put(buf)
		return info.Size, nil
	case storage.StorageResourceState:
		// States and locks are uploaded straight from `rd` with size -1,
		// meaning "unknown size, please figure it out as you stream it" -
		// minio-go handles this by buffering internally as needed. We don't
		// bother with the bufPool trick here since states/locks are
		// typically much smaller and less frequent than packfiles.
		info, err := s.minioClient.PutObject(ctx, s.bucket, s.realpath(fmt.Sprintf("states/%02x/%016x", mac[0], mac)), rd, -1, s.putObjectOptions)
		if err != nil {
			return 0, fmt.Errorf("put %s object: %w", res, err)
		}

		return info.Size, nil
	case storage.StorageResourceLock:
		info, err := s.minioClient.PutObject(ctx, s.bucket, s.realpath(fmt.Sprintf("locks/%016x", mac)), rd, -1, s.putObjectOptions)
		if err != nil {
			return 0, fmt.Errorf("put %s object: %w", res, err)
		}
		return info.Size, nil
	}

	return -1, errors.ErrUnsupported
}

// Get downloads one object identified by its MAC. If rg (a byte range) is
// given, only that portion of the object is returned instead of the whole
// thing - used e.g. to fetch just one small piece out of a large packfile
// instead of downloading it in full.
func (s *Store) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	var path string
	switch res {
	case storage.StorageResourcePackfile:
		path = s.realpath(fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac))
	case storage.StorageResourceState:
		path = s.realpath(fmt.Sprintf("states/%02x/%016x", mac[0], mac))
	case storage.StorageResourceLock:
		path = s.realpath(fmt.Sprintf("locks/%016x", mac))
	default:
		return nil, errors.ErrUnsupported
	}

	object, err := s.minioClient.GetObject(ctx, s.bucket, path, minio.GetObjectOptions{ServerSideEncryption: s.ssec})
	if err != nil {
		return nil, fmt.Errorf("get %s object: %w", res, err)
	}

	if rg != nil {
		// Wrap the full object stream so that reading from it only ever
		// exposes the requested [offset, offset+length) slice, as if that
		// were the entire file.
		return reading.NewSectionReadCloser(object, int64(rg.Offset), int64(rg.Length)), nil
	}

	return object, nil
}

// Delete removes one object identified by its MAC. Used e.g. when pruning
// old snapshots or removing an expired lock.
func (s *Store) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	var path string
	switch res {
	case storage.StorageResourcePackfile:
		path = s.realpath(fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac))
	case storage.StorageResourceState:
		path = s.realpath(fmt.Sprintf("states/%02x/%016x", mac[0], mac))
	case storage.StorageResourceLock:
		path = s.realpath(fmt.Sprintf("locks/%016x", mac))
	}

	err := s.minioClient.RemoveObject(ctx, s.bucket, path, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("remove %s object: %w", res, err)
	}
	return nil
}

// Close is called when Plakar is done with this repository for now. There's
// no persistent connection or file handle this connector needs to release
// (each B2 request is just an independent HTTPS call), so there's nothing
// to do here.
func (s *Store) Close(ctx context.Context) error {
	return nil
}
