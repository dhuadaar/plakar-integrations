#!/usr/bin/env bash
# End-to-end test for the backblazeb2 (native B2 API) storage connector.
#
# Self-contained: this module owns its own copy of the e2e driver rather
# than sourcing a shared script, so it can be run/read/modified independently.
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
#
# Location URL form:
#   b2://<bucket>/<optional-prefix>
#
# Existing/new behavior:
# - If B2_PREFIX is provided, the script will reuse/open an existing repository
#   at that prefix when already initialized.
# - If B2_PREFIX is empty, the script auto-generates a fresh prefix and creates
#   a new repository.

set -euo pipefail

# --- module-specific constants -------------------------------------------
# Connector short name used in temp dir naming and logs.
CONNECTOR_NAME="native"
# Human-readable plugin label for script output.
PLUGIN_LABEL="backblazeb2 storage plugin"
# Package name used by `plakar pkg add/rm`.
PLUGIN_PKG_NAME="backblazeb2"
# URL scheme used when building store location.
LOCATION_SCHEME="b2"
# --------------------------------------------------------------------------

# Absolute path to this script directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Module root directory (one level above scripts/).
MODULE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Plakar release version to download when not already available.
PLAKAR_VERSION="${PLAKAR_VERSION:-v1.1.4}"
# Set to 1 to force deleting and re-downloading plakar binary.
FORCE_REDOWNLOAD_PLAKAR="${FORCE_REDOWNLOAD_PLAKAR:-0}"
# Set to 1 to keep temporary workdir after script exits.
KEEP_TMP="${KEEP_TMP:-0}"

# Root temporary working directory for this run.
WORKDIR=""
# Plakar config directory inside WORKDIR.
CONF_DIR=""
# Plakar cache directory inside WORKDIR.
CACHE_DIR=""
# Source test fixture directory populated by script.
TEST_DIR=""
# Restore destination for newly created snapshot.
RESTORE_DIR=""
# Restore destination for a pre-existing snapshot (existing repo flow).
EXISTING_RESTORE_DIR=""
# Directory where cached downloaded plakar binary is stored.
BIN_DIR=""
# Resolved plakar executable path used by helper function.
PLAKAR_BIN=""
# Computed store location URL (b2://bucket/prefix).
LOCATION=""

# Snapshot ID that existed before this run (existing repo flow).
PREV_SNAPSHOT_ID=""
# Count of snapshots that existed before this run.
PREV_SNAPSHOT_COUNT=0
# Flag: 1 when script reused an existing repository.
USED_EXISTING_REPO=0
# Snapshot IDs captured before creating backup in this run.
BEFORE_SNAPSHOT_IDS=""
# Snapshot ID created by this run.
SNAPSHOT_ID=""
# Flag: 1 when B2_PREFIX was explicitly provided by caller/env file.
PREFIX_WAS_PROVIDED=0

cleanup() {
	if [ -z "$WORKDIR" ]; then
		return
	fi
	if [ "$KEEP_TMP" = "1" ]; then
		echo "==> KEEP_TMP=1, leaving workdir: $WORKDIR"
	else
		rm -rf "$WORKDIR"
	fi
}
trap cleanup EXIT

die() {
	echo "!! $*" >&2
	exit 1
}

preserve_and_load_env() {
	for _v in B2_PREFIX; do
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

	for _v in B2_PREFIX; do
		_caller_var="_caller_${_v}"
		if [ "${!_caller_var+set}" = "set" ]; then
			eval "$_v=\${$_caller_var}"
		fi
	done
	unset _v _caller_var _caller_B2_PREFIX
}

validate_and_normalize_env() {
	: "${B2_BUCKET:?set B2_BUCKET (see scripts/b2-creds.env.example)}"
	: "${B2_ACCESS_KEY_ID:?set B2_ACCESS_KEY_ID (see scripts/b2-creds.env.example)}"
	: "${B2_APPLICATION_KEY:?set B2_APPLICATION_KEY (see scripts/b2-creds.env.example)}"

	# Tolerate CRLF-edited creds files.
	B2_BUCKET="${B2_BUCKET%$'\r'}"
	B2_ACCESS_KEY_ID="${B2_ACCESS_KEY_ID%$'\r'}"
	B2_APPLICATION_KEY="${B2_APPLICATION_KEY%$'\r'}"
	B2_PREFIX="${B2_PREFIX%$'\r'}"

	if [ -n "${B2_PREFIX:-}" ]; then
		PREFIX_WAS_PROVIDED=1
	else
		B2_PREFIX="plakar-test-$(date +%s)-$$"
		PREFIX_WAS_PROVIDED=0
	fi
}

setup_workdirs() {
	WORKDIR="$(mktemp -d "/tmp/b2e2e-${CONNECTOR_NAME}.XXXXXX")"
	CONF_DIR="$WORKDIR/config"
	CACHE_DIR="$WORKDIR/cache"
	TEST_DIR="$WORKDIR/testdata"
	RESTORE_DIR="$WORKDIR/restored"
	EXISTING_RESTORE_DIR="$WORKDIR/restored-existing"
	BIN_DIR="$MODULE_ROOT/.bin"
	mkdir -p "$CONF_DIR" "$CACHE_DIR" "$TEST_DIR/subdir" "$RESTORE_DIR" "$EXISTING_RESTORE_DIR" "$BIN_DIR"
	echo "==> Workdir: $WORKDIR"
}

validate_b2_credentials() {
	echo "==> Validating B2 credentials (authorize account)"
	local auth_body auth_code
	auth_body="$WORKDIR/b2_authorize_account.json"
	auth_code="$(curl -sS -o "$auth_body" -w "%{http_code}" \
		-u "$B2_ACCESS_KEY_ID:$B2_APPLICATION_KEY" \
		"https://api.backblazeb2.com/b2api/v2/b2_authorize_account" || true)"
	if [ "$auth_code" != "200" ]; then
		echo "!! b2_authorize_account failed with HTTP $auth_code" >&2
		if [ "$auth_code" = "401" ]; then
			echo "!! invalid B2 keyID/applicationKey pair (or revoked key)." >&2
			echo "!! ensure B2_ACCESS_KEY_ID and B2_APPLICATION_KEY come from the same app key." >&2
		fi
		echo "!! response body:" >&2
		cat "$auth_body" >&2 || true
		exit 1
	fi
}

ensure_plakar_binary() {
	PLAKAR_BIN="$BIN_DIR/plakar"
	local system_plakar

	if [ "$FORCE_REDOWNLOAD_PLAKAR" = "1" ]; then
		echo "==> FORCE_REDOWNLOAD_PLAKAR=1, removing cached plakar binary"
		rm -f "$PLAKAR_BIN"
	fi

	# `command -v` can return a shell function/alias name (e.g. just "plakar"),
	# which may recurse/hang. Use `type -P` so we only accept a real executable.
	system_plakar="$(type -P plakar 2>/dev/null || true)"
	if [ -n "$system_plakar" ] && [ "$FORCE_REDOWNLOAD_PLAKAR" != "1" ]; then
		PLAKAR_BIN="$system_plakar"
		echo "==> Using plakar already on PATH: $PLAKAR_BIN"
	elif [ -x "$PLAKAR_BIN" ]; then
		echo "==> Using cached plakar: $PLAKAR_BIN"
	else
		echo "==> Downloading plakar $PLAKAR_VERSION"
		local os arch asset url
		os="$(uname -s | tr '[:upper:]' '[:lower:]')"
		arch="$(uname -m)"
		case "$arch" in
		x86_64) arch=amd64 ;;
		aarch64) arch=arm64 ;;
		esac
		asset="plakar_${PLAKAR_VERSION#v}_${os}_${arch}.tar.gz"
		url="https://github.com/PlakarKorp/plakar/releases/download/${PLAKAR_VERSION}/${asset}"
		curl -sL -o "$WORKDIR/plakar.tar.gz" "$url"
		tar -xzf "$WORKDIR/plakar.tar.gz" -C "$WORKDIR" plakar
		install -m 0755 "$WORKDIR/plakar" "$PLAKAR_BIN"
	fi

	# Use isolated config/cache so local/global plakar state does not interfere
	# with this test run.
	"$PLAKAR_BIN" -configdir "$CONF_DIR" -cachedir "$CACHE_DIR" version
}

plakar() {
	# Centralized plakar wrapper: every invocation uses the same ephemeral
	# config/cache dirs for deterministic test behavior.
	"$PLAKAR_BIN" -configdir "$CONF_DIR" -cachedir "$CACHE_DIR" "$@"
}

build_and_install_plugin() {
	# Remove previously installed package in the isolated plakar profile so
	# reinstall always picks up the plugin built from current workspace state.
	plakar pkg rm "$PLUGIN_PKG_NAME" >/dev/null 2>&1 || true
	LOCATION="${LOCATION_SCHEME}://${B2_BUCKET}/${B2_PREFIX}"
	echo "==> Location: $LOCATION"

	echo "==> Building and installing the ${PLUGIN_LABEL}"
	# Clean artifacts to avoid stale binaries/archives across consecutive runs.
	rm -f "$MODULE_ROOT/b2Storage" "$MODULE_ROOT/backblazeb2_v1.1.0_darwin_arm64.ptar"
	# `make reinstall` builds plugin binary, packages ptar from manifest, then
	# installs it into plakar package registry.
	make -C "$MODULE_ROOT" reinstall \
		PLAKAR="$PLAKAR_BIN -configdir $CONF_DIR -cachedir $CACHE_DIR"
}

populate_test_directory() {
	echo "==> Populating test directory: $TEST_DIR"
	printf 'Hello, Backblaze B2!\nThis is a plain text file.\n' >"$TEST_DIR/hello.txt"
	cat >"$TEST_DIR/data.json" <<'EOF'
{"name": "plakar", "backend": "backblazeb2", "count": 3, "nested": {"ok": true}}
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
}

configure_store() {
	echo "==> Store location: $LOCATION"
	# Registers a named store alias (`b2test`) in plakar config; all subsequent
	# `plakar at @b2test ...` commands operate through this connector config.
	plakar store add b2test "$LOCATION" \
		access_key="$B2_ACCESS_KEY_ID" \
		secret_access_key="$B2_APPLICATION_KEY"
}

handle_create_failure() {
	local create_output="$1"
	local create_status="$2"

	if printf '%s' "$create_output" | grep -qi "already initialized"; then
		if [ "$PREFIX_WAS_PROVIDED" -ne 1 ]; then
			die "repository already initialized at auto-generated prefix '$B2_PREFIX' (unexpected). set an explicit B2_PREFIX to reuse existing repos."
		fi

		echo "==> Repository already exists at this location."
		PREV_SNAPSHOT_ID="$(plakar at @b2test ls | awk 'NF{print $2; exit}')"
		PREV_SNAPSHOT_COUNT="$(plakar at @b2test ls | awk 'NF{c++} END{print c+0}')"
		USED_EXISTING_REPO=1
		plakar at @b2test ls >/dev/null
		return
	fi

	echo "!! failed to create repository" >&2
	if printf '%s' "$create_output" | grep -q "403"; then
		echo "!! received HTTP 403 from B2 during create." >&2
		echo "!! check app key permissions (readFiles + writeFiles), bucket scope, and allowed prefix." >&2
		echo "!! current prefix: '$B2_PREFIX'" >&2
		echo "!! if the key is prefix-restricted, set B2_PREFIX to that allowed prefix in scripts/b2-creds.env." >&2
	fi
	exit "$create_status"
}

create_or_open_repository() {
	echo "==> Creating the Kloset repository on B2"
	PREV_SNAPSHOT_ID=""
	PREV_SNAPSHOT_COUNT=0
	USED_EXISTING_REPO=0
	# Capture current snapshot IDs first so we can reliably identify the new
	# snapshot created by this run even when repository already contains data.
	BEFORE_SNAPSHOT_IDS="$(plakar at @b2test ls 2>/dev/null | awk 'NF{print $2}' || true)"

	local create_output create_status
	set +e
	create_output="$(plakar at @b2test create -plaintext 2>&1)"
	create_status=$?
	set -e
	echo "$create_output"

	if [ "$create_status" -ne 0 ]; then
		handle_create_failure "$create_output" "$create_status"
	fi
}

backup_and_pick_snapshot() {
	echo "==> Backing up $TEST_DIR"
	plakar at @b2test backup "$TEST_DIR"

	local after_snapshot_ids
	after_snapshot_ids="$(plakar at @b2test ls | awk 'NF{print $2}')"
	# Compute set difference (after - before) to obtain the snapshot generated
	# by this run. Fallback to top entry if list format/order changes.
	SNAPSHOT_ID="$(comm -23 <(printf '%s\n' "$after_snapshot_ids" | sort) <(printf '%s\n' "$BEFORE_SNAPSHOT_IDS" | sort) | head -1)"
	if [ -z "$SNAPSHOT_ID" ]; then
		SNAPSHOT_ID="$(printf '%s\n' "$after_snapshot_ids" | head -1)"
	fi
	if [ -z "$SNAPSHOT_ID" ]; then
		die "could not determine snapshot id"
	fi
	echo "==> Snapshot ID: $SNAPSHOT_ID"
}

verify_existing_snapshot_if_needed() {
	if [ "$USED_EXISTING_REPO" -ne 1 ]; then
		return
	fi

	if [ "$PREV_SNAPSHOT_COUNT" -le 0 ] || [ -z "$PREV_SNAPSHOT_ID" ]; then
		die "expected existing snapshots, but none were found"
	fi

	echo "==> Verifying existing snapshot is still accessible: $PREV_SNAPSHOT_ID"
	# Restore the previously existing snapshot to prove repository history is
	# still readable after adding new data.
	plakar at @b2test restore -to "$EXISTING_RESTORE_DIR" "$PREV_SNAPSHOT_ID"

	local existing_file_count
	existing_file_count="$(find "$EXISTING_RESTORE_DIR" -type f | wc -l | tr -d ' ')"
	if [ "$existing_file_count" -le 0 ]; then
		die "existing repository verification failed: no files restored from previous snapshot"
	fi
	echo "==> Existing files verification: restored $existing_file_count pre-existing files"
}

check_restore_and_diff() {
	echo "==> Checking repository integrity"
	# Connector-level consistency check (object graph/content validation).
	plakar at @b2test check

	echo "==> Restoring snapshot to $RESTORE_DIR"
	# Restore only the backed-up source subtree and compare bytes on disk.
	plakar at @b2test restore -to "$RESTORE_DIR" "$SNAPSHOT_ID:$TEST_DIR"

	echo "==> Diffing $TEST_DIR vs $RESTORE_DIR"
	if ! diff -r "$TEST_DIR" "$RESTORE_DIR"; then
		echo
		die "restored directory differs from the original"
	fi

	echo
	echo "PASS: restored directory is identical to the original"
	if [ "$USED_EXISTING_REPO" -eq 1 ]; then
		echo "      existing snapshot content was also verified from $PREV_SNAPSHOT_ID"
	fi
	echo "      (B2 objects were written under prefix '$B2_PREFIX' in bucket '$B2_BUCKET' - not deleted automatically)"
}

main() {
	preserve_and_load_env
	validate_and_normalize_env
	setup_workdirs
	validate_b2_credentials
	ensure_plakar_binary
	build_and_install_plugin
	populate_test_directory
	configure_store
	create_or_open_repository
	backup_and_pick_snapshot
	verify_existing_snapshot_if_needed
	check_restore_and_diff
}

main "$@"
