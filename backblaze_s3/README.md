# Backblaze B2 Integration (S3-compatible)

## Overview

**Backblaze B2** is a low-cost, S3-adjacent object storage service. This
integration talks to B2 through its **S3-compatible endpoint** (via the
[`minio-go`](https://github.com/minio/minio-go) client), so it behaves like
any other S3-family Kloset storage backend. If you'd rather avoid the S3
compatibility layer and talk to B2's native API directly, see the
`backblazeb2` integration instead.

This integration allows:

* **Storing a full Kloset repository in a B2 bucket:**
  Use Backblaze B2 (via its S3-compatible endpoint) as the storage backend
  for a Kloset repository (packfiles, states, locks, and the repository
  CONFIG).

* **Sharing one bucket across multiple repositories:**
  An optional in-bucket prefix lets several Kloset repositories live safely
  side by side in the same B2 bucket, each under its own prefix.

* **Server-side encryption (SSE-C):**
  An optional customer-provided AES-256 key can be supplied so objects are
  encrypted at rest using SSE-C.

## Configuration

The configuration parameters are as follows:

- `location` (required): The B2 S3-compatible location URI, in the form
  `b2s3://<endpoint>/<bucket>/<optional-prefix>`, e.g.
  `b2s3://s3.us-west-004.backblazeb2.com/mybucket/myprefix` or
  `b2s3://s3.us-west-004.backblazeb2.com/mybucket` with no prefix. The
  endpoint is your bucket's actual B2 S3-compatible endpoint (region-specific)
  and is dialed directly, unlike the `backblazeb2` native connector.
- `access_key` (required): B2 application key ID
- `secret_access_key` (required): B2 application key
- `sse_customer_key` (optional): Base64-encoded 256-bit (32-byte)
  customer-provided key for SSE-C server-side encryption

## Examples

```bash
# Configure a B2 store using the S3-compatible endpoint
$ plakar store add myB2store b2s3://s3.us-west-004.backblazeb2.com/mybucket/myprefix \
    access_key=YOUR_KEY_ID secret_access_key=YOUR_APP_KEY

# Create the store
$ plakar at @myB2store create

# Backup a directory into the store
$ plakar at @myB2store backup /path/to/data

# Check repository integrity
$ plakar at @myB2store check

# Restore a snapshot
$ plakar at @myB2store restore -to /path/to/restore <snapid>
```

## Running the end-to-end tests

`scripts/test-e2e.sh` is a self-contained script that builds and installs
the plugin, then drives a real `plakar` binary against a real B2 bucket
(create/backup/check/restore/diff) over the S3-compatible endpoint. It
requires a B2 application key and endpoint - nothing is mocked.

```bash
# One-time setup: fill in real credentials
$ cp scripts/b2-creds.env.example scripts/b2-creds.env
$ vi scripts/b2-creds.env

# Run it
$ scripts/test-e2e.sh
```

Behavior is controlled by env vars (settable in `b2-creds.env` or on the
command line, command-line wins):

- `B2_PREFIX`: leave empty (default) to always exercise the **new-repo**
  path against a freshly generated prefix; set it to a fixed value to
  exercise the **existing-repo** path on subsequent runs (the script detects
  "already initialized" and, after confirmation, adds a new snapshot while
  also verifying the previous snapshot is still restorable).
- `B2_ENDPOINT`: your bucket's B2 S3-compatible endpoint, e.g.
  `s3.us-west-004.backblazeb2.com`.

```bash
# New repo
$ scripts/test-e2e.sh

# Existing repo (run twice with the same fixed prefix)
$ B2_PREFIX=plakar-existing-test scripts/test-e2e.sh
$ B2_PREFIX=plakar-existing-test scripts/test-e2e.sh   # add to existing repo when prompted
```
