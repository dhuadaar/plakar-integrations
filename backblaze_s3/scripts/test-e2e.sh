#!/usr/bin/env bash
# End-to-end test for the backblaze_s3 (S3-compatible endpoint) storage connector.
#
# Self-contained: this module owns its own copy of the e2e driver rather
# than sourcing a shared script, so it can be run/read/modified independently
# of the backblazeb2 (native API) module. The two copies are expected to
# drift slightly over time (e.g. connector-specific location URIs and the
# S3-endpoint sanity check below) - that's fine, keep them independent
# rather than re-introducing a shared file.
#
# Exercises both configuration states in one run:
#   - "new"      repo: if nothing exists yet at LOCATION, `create` succeeds
#                and a fresh repository is initialized and backed up into.
#   - "existing" repo: if `create` reports "already initialized" (i.e. a
#                previous run left a repo at this prefix), you'll be asked
#                whether to add a new snapshot into the existing repository;
#                on "yes" the script also verifies the previously existing
#                snapshot is still readable, proving old data survived.
#
# Usage:
#   cp scripts/b2-creds.env.example scripts/b2-creds.env   # fill in real values
#   scripts/test-e2e.sh
#
# To force a brand-new repository every run, leave B2_PREFIX empty in
# b2-creds.env (auto-generates a unique prefix). To reuse the same repo
# across runs (to exercise the "existing" path), set B2_PREFIX to a fixed
# value such as "plakar".

set -euo pipefail

# --- module-specific constants -------------------------------------------
CONNECTOR_NAME="s3"
PLUGIN_LABEL="backblaze_s3 storage plugin"
PLUGIN_PKG_NAME="backblazes3"
LOCATION_SCHEME="b2s3"
# --------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Preserve any of these vars if the caller already set them in the
# environment (e.g. `B2_PREFIX=foo ./test-e2e.sh` to force a fixed run) - the
# creds file below is meant to supply defaults, not clobber explicit
# invocation-time overrides.
for _v in B2_PREFIX B2_ENDPOINT; do
	if [ "${!_v+set}" = "set" ]; then
		eval "_caller_${_v}=\${$_v}"
	fi
done

CREDS_FILE="$SCRIPT_DIR/b2-creds.env"
if [ -f "$CREDS_FILE" ]; then
	# shellcheck disable=SC1090
	set -a
	source "$CREDS_FILE"
	set +a
fi

for _v in B2_PREFIX B2_ENDPOINT; do
	_caller_var="_caller_${_v}"
	if [ "${!_caller_var+set}" = "set" ]; then
		eval "$_v=\${$_caller_var}"
	fi
done
unset _v _caller_var _caller_B2_PREFIX _caller_B2_ENDPOINT

: "${B2_BUCKET:?set B2_BUCKET (see scripts/b2-creds.env.example)}"
: "${B2_ACCESS_KEY_ID:?set B2_ACCESS_KEY_ID (see scripts/b2-creds.env.example)}"
: "${B2_APPLICATION_KEY:?set B2_APPLICATION_KEY (see scripts/b2-creds.env.example)}"
: "${B2_ENDPOINT:?set B2_ENDPOINT (see scripts/b2-creds.env.example)}"
B2_PREFIX="${B2_PREFIX:-plakar-test-$(date +%s)-$$}"

B2_ENDPOINT="${B2_ENDPOINT#http://}"
B2_ENDPOINT="${B2_ENDPOINT#https://}"
B2_ENDPOINT="${B2_ENDPOINT%/}"
case "$B2_ENDPOINT" in
api*.backblazeb2.com)
	echo "!! B2_ENDPOINT looks like the native B2 API endpoint ($B2_ENDPOINT)." >&2
	echo "!! This connector needs the S3-compatible endpoint instead, e.g. s3.us-west-004.backblazeb2.com." >&2
	echo "!! Find it on the bucket details page in the B2 console (labeled \"Endpoint\")." >&2
	exit 1
	;;
esac

PLAKAR_VERSION="${PLAKAR_VERSION:-v1.1.4}"
WORKDIR="$(mktemp -d "/tmp/b2e2e-${CONNECTOR_NAME}.XXXXXX")"
KEEP_TMP="${KEEP_TMP:-0}"
cleanup() {
	if [ "$KEEP_TMP" = "1" ]; then
		echo "==> KEEP_TMP=1, leaving workdir: $WORKDIR"
	else
		rm -rf "$WORKDIR"
	fi
}
trap cleanup EXIT

CONF_DIR="$WORKDIR/config"
CACHE_DIR="$WORKDIR/cache"
TEST_DIR="$WORKDIR/testdata"
RESTORE_DIR="$WORKDIR/restored"
EXISTING_RESTORE_DIR="$WORKDIR/restored-existing"
BIN_DIR="$MODULE_ROOT/.bin"
mkdir -p "$CONF_DIR" "$CACHE_DIR" "$TEST_DIR/subdir" "$RESTORE_DIR" "$EXISTING_RESTORE_DIR" "$BIN_DIR"

echo "==> Workdir: $WORKDIR"

PLAKAR_BIN="$BIN_DIR/plakar"
if command -v plakar >/dev/null 2>&1; then
	PLAKAR_BIN="$(command -v plakar)"
	echo "==> Using plakar already on PATH: $PLAKAR_BIN"
elif [ -x "$PLAKAR_BIN" ]; then
	echo "==> Using cached plakar: $PLAKAR_BIN"
else
	echo "==> Downloading plakar $PLAKAR_VERSION"
	OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
	ARCH="$(uname -m)"
	case "$ARCH" in
	x86_64) ARCH=amd64 ;;
	aarch64) ARCH=arm64 ;;
	esac
	ASSET="plakar_${PLAKAR_VERSION#v}_${OS}_${ARCH}.tar.gz"
	URL="https://github.com/PlakarKorp/plakar/releases/download/${PLAKAR_VERSION}/${ASSET}"
	curl -sL -o "$WORKDIR/plakar.tar.gz" "$URL"
	tar -xzf "$WORKDIR/plakar.tar.gz" -C "$WORKDIR" plakar
	install -m 0755 "$WORKDIR/plakar" "$PLAKAR_BIN"
fi
"$PLAKAR_BIN" version

plakar() {
	"$PLAKAR_BIN" -configdir "$CONF_DIR" -cachedir "$CACHE_DIR" "$@"
}

# Distinct protocol schemes (b2 vs b2s3) mean the native and S3-compatible
# plugins can coexist installed - no need to uninstall the other module's
# package before installing this one.
plakar pkg rm "$PLUGIN_PKG_NAME" >/dev/null 2>&1 || true
LOCATION="${LOCATION_SCHEME}://${B2_ENDPOINT}/${B2_BUCKET}/${B2_PREFIX}"

echo "==> Building and installing the ${PLUGIN_LABEL}"
make -C "$MODULE_ROOT" reinstall \
	PLAKAR="$PLAKAR_BIN -configdir $CONF_DIR -cachedir $CACHE_DIR"

echo "==> Populating test directory: $TEST_DIR"
printf 'Hello, Backblaze B2!\nThis is a plain text file.\n' >"$TEST_DIR/hello.txt"
cat >"$TEST_DIR/data.json" <<'EOF'
{"name": "plakar", "backend": "backblaze_s3", "count": 3, "nested": {"ok": true}}
EOF
cat >"$TEST_DIR/table.csv" <<'EOF'
id,name,value
1,alpha,10
2,beta,20
3,gamma,30
EOF
cat >"$TEST_DIR/README.md" <<'EOF'
# Test fixture

Some **markdown** content used for the backup/restore roundtrip test.
EOF
head -c 65536 /dev/urandom >"$TEST_DIR/subdir/random.bin"
cat >"$TEST_DIR/subdir/notes.txt" <<'EOF'
Nested file, to make sure directory structure is preserved.
EOF

echo "==> Store location: $LOCATION"
plakar store add b2test "$LOCATION" \
	access_key="$B2_ACCESS_KEY_ID" \
	secret_access_key="$B2_APPLICATION_KEY"

echo "==> Creating the Kloset repository on B2"
PREV_SNAPSHOT_ID=""
PREV_SNAPSHOT_COUNT=0
USED_EXISTING_REPO=0
BEFORE_SNAPSHOT_IDS="$(plakar at @b2test ls 2>/dev/null | awk 'NF{print $2}' || true)"

set +e
CREATE_OUTPUT="$(plakar at @b2test create -plaintext 2>&1)"
CREATE_STATUS=$?
set -e
echo "$CREATE_OUTPUT"

if [ "$CREATE_STATUS" -ne 0 ]; then
	if printf '%s' "$CREATE_OUTPUT" | grep -qi "already initialized"; then
		echo "==> Repository already exists at this location."
		PREV_SNAPSHOT_ID="$(plakar at @b2test ls | awk 'NF{print $2; exit}')"
		PREV_SNAPSHOT_COUNT="$(plakar at @b2test ls | awk 'NF{c++} END{print c+0}')"

		read -r -p "Add to existing repository and open it? (y/n): " USE_EXISTING
		case "$USE_EXISTING" in
		y|Y|yes|YES|Yes)
			USED_EXISTING_REPO=1
			plakar at @b2test ls >/dev/null
			;;
		*)
			echo "Aborted by user (chose not to add to existing repository)." >&2
			exit 1
			;;
		esac
	else
		echo "!! failed to create repository" >&2
		exit "$CREATE_STATUS"
	fi
fi

echo "==> Backing up $TEST_DIR"
plakar at @b2test backup "$TEST_DIR"

AFTER_SNAPSHOT_IDS="$(plakar at @b2test ls | awk 'NF{print $2}')"
SNAPSHOT_ID="$(comm -23 <(printf '%s\n' "$AFTER_SNAPSHOT_IDS" | sort) <(printf '%s\n' "$BEFORE_SNAPSHOT_IDS" | sort) | head -1)"
if [ -z "$SNAPSHOT_ID" ]; then
	SNAPSHOT_ID="$(printf '%s\n' "$AFTER_SNAPSHOT_IDS" | head -1)"
fi
if [ -z "$SNAPSHOT_ID" ]; then
	echo "!! could not determine snapshot id" >&2
	exit 1
fi
echo "==> Snapshot ID: $SNAPSHOT_ID"

if [ "$USED_EXISTING_REPO" -eq 1 ]; then
	if [ "$PREV_SNAPSHOT_COUNT" -le 0 ] || [ -z "$PREV_SNAPSHOT_ID" ]; then
		echo "!! expected existing snapshots, but none were found" >&2
		exit 1
	fi

	echo "==> Verifying existing snapshot is still accessible: $PREV_SNAPSHOT_ID"
	plakar at @b2test restore -to "$EXISTING_RESTORE_DIR" "$PREV_SNAPSHOT_ID"

	EXISTING_FILE_COUNT="$(find "$EXISTING_RESTORE_DIR" -type f | wc -l | tr -d ' ')"
	if [ "$EXISTING_FILE_COUNT" -le 0 ]; then
		echo "!! existing repository verification failed: no files restored from previous snapshot" >&2
		exit 1
	fi
	echo "==> Existing files verification: restored $EXISTING_FILE_COUNT pre-existing files"
fi

echo "==> Checking repository integrity"
plakar at @b2test check

echo "==> Restoring snapshot to $RESTORE_DIR"
plakar at @b2test restore -to "$RESTORE_DIR" "$SNAPSHOT_ID:$TEST_DIR"

echo "==> Diffing $TEST_DIR vs $RESTORE_DIR"
if diff -r "$TEST_DIR" "$RESTORE_DIR"; then
	echo
	echo "PASS: restored directory is identical to the original"
	if [ "$USED_EXISTING_REPO" -eq 1 ]; then
		echo "      existing snapshot content was also verified from $PREV_SNAPSHOT_ID"
	fi
	echo "      (B2 objects were written under prefix '$B2_PREFIX' in bucket '$B2_BUCKET' - not deleted automatically)"
	exit 0
else
	echo
	echo "FAIL: restored directory differs from the original" >&2
	exit 1
fi
