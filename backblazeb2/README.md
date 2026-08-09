# Backblaze B2 Integration

## Overview

**Backblaze B2** is a low-cost, S3-adjacent object storage service. This
integration talks to B2 through its **native API** (via the
[`blazer`](https://github.com/Backblaze/blazer) SDK), rather than through
B2's S3-compatible endpoint. If you'd rather use the S3-compatible endpoint
(e.g. for parity with existing S3 tooling), see the `backblaze_s3` integration
instead.

This integration allows:

* **Storing a full Kloset repository in a B2 bucket:**
  Use Backblaze B2 as the storage backend for a Kloset repository (packfiles,
  states, locks, and the repository CONFIG), addressed directly through B2's
  native API.

* **Sharing one bucket across multiple repositories:**
  An optional in-bucket prefix lets several Kloset repositories live safely
  side by side in the same B2 bucket, each under its own prefix.


## Configuration

The configuration parameters are as follows:

- `location` (required): The B2 location URI. Two forms are supported:
  - **Native (preferred):** `b2://<bucket>/<optional-prefix>`, e.g.
    `b2://mybucket/myprefix` or just `b2://mybucket` with no prefix.
  - **Legacy/compatible:** `b2://<endpoint>/<bucket>/<optional-prefix>`, e.g.
    `b2://s3.us-west-004.backblazeb2.com/mybucket` (the endpoint segment is
    accepted for backward compatibility but is not otherwise used, since this
    connector always talks to B2's native API, not the given endpoint).
- `access_key` (required): B2 application key ID
- `secret_access_key` (required): B2 application key

## Examples

```bash
# Configure a B2 store using the native (preferred) location form
$ plakar store add myB2store b2://mybucket/myprefix access_key=YOUR_KEY_ID secret_access_key=YOUR_APP_KEY

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
(create/backup/check/restore/diff). It requires a B2 application key -
nothing is mocked.

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
- `B2_LOCATION_FORM`: `native` (default) or `legacy` - selects which of the
  two supported `location` URL forms (see Configuration above) the run
  exercises.
- `B2_LEGACY_ENDPOINT`: only used when `B2_LOCATION_FORM=legacy`; any
  dotted hostname works, since the native connector never dials it (see
  Configuration above).

```bash
# New repo, native URL form (default)
$ scripts/test-e2e.sh

# Existing repo, native URL form (run twice with the same fixed prefix)
$ B2_PREFIX=plakar-existing-test scripts/test-e2e.sh
$ B2_PREFIX=plakar-existing-test scripts/test-e2e.sh   # add to existing repo when prompted

# New repo, legacy URL form
$ B2_LOCATION_FORM=legacy scripts/test-e2e.sh

# Existing repo, legacy URL form
$ B2_LOCATION_FORM=legacy B2_PREFIX=plakar-existing-legacy scripts/test-e2e.sh
$ B2_LOCATION_FORM=legacy B2_PREFIX=plakar-existing-legacy scripts/test-e2e.sh
```
