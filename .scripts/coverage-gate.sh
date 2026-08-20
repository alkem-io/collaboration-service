#!/usr/bin/env bash
# coverage-gate.sh — combined coverage measurement + ≥95% gate (T017.5, SC-008/SC-011).
#
# The collaboration-service is a hexagon whose behaviour is proven across three
# complementary lanes:
#   - unit         (default build) — domain + adapter logic with in-process fakes
#   - integration  (-tags integration) — adapter live paths against real backends
#                  (redis/postgres/rabbitmq) + the app.New durable wiring
#   - e2e          (-tags e2e) — the full service through app.New over real
#                  WebSockets, incl. the JS-client y-protocols interop harness
#
# A statement covered by ANY lane counts, so this script runs all available lanes
# (integration is gated on its backend env vars being set; when they are unset
# those tests t.Skip and the lane still runs the unit tests it shares a binary
# with), merges their profiles, and enforces the threshold on the MEANINGFUL
# business-logic scope.
#
# Scope (constitution §XII "Meaningful Tests Only" — never pad, never test code
# that cannot fail): the gate excludes the process entrypoints under cmd/ (flag
# parsing, dialling, and the HTTP-server lifecycle — exercised by running the
# binary, not unit-testable without launching a process) and the zap logger
# constructor. Pure observability no-op sinks (NopMetrics) are likewise out of
# scope — including lifecycle.NopObserver, kept in observer_nop.go for exactly
# this reason. Everything else — every domain rule and adapter — must clear the bar.
#
# The exclusion covers the entrypoint only, never the logic behind it. cmd/
# lifecycle-replay is a flag parser in front of lifecycle.Replay, and Replay
# itself is inside the bar and covered against a real broker.
#
# Usage:
#   .scripts/coverage-gate.sh [threshold]
# Env (optional; integration lane backends — unset ⇒ those tests skip):
#   POSTGRES_TEST_DSN, RABBITMQ_TEST_URL

set -euo pipefail

THRESHOLD="${1:-95.0}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COVDIR="$(mktemp -d)"
trap 'rm -rf "$COVDIR"' EXIT

# Coverage is attributed to every production package (excluding the e2e test
# package itself, which carries no production statements).
COVERPKG="$(go list ./... | grep -v '/test/e2e$' | paste -sd, -)"

# Read GOTESTFLAGS into an array so a multi-flag value (e.g. "-count=1 -v
# -timeout 5m") is word-split into separate args, not passed as one token, while
# still being safe under set -u.
read -r -a GOTESTFLAGS <<< "${GOTESTFLAGS:--count=1}"

echo "==> unit + integration lane (-tags integration)"
# The integration tag is additive: this single invocation runs the unit tests AND
# the build-tagged integration tests (which t.Skip when their backend env is unset).
go test "${GOTESTFLAGS[@]}" -tags integration -coverpkg="$COVERPKG" \
  -coverprofile="$COVDIR/integration.cov" ./...

echo "==> e2e lane (-tags e2e)"
go test "${GOTESTFLAGS[@]}" -tags e2e -coverpkg="$COVERPKG" \
  -coverprofile="$COVDIR/e2e.cov" ./test/e2e/...

echo "==> merging coverage profiles"
go run github.com/wadey/gocovmerge@v0.0.0-20160331181800-b5bfa59ec0ad \
  "$COVDIR/integration.cov" "$COVDIR/e2e.cov" > "$COVDIR/merged.cov"
# Persist the merged profile at the repo root so CI can upload it as an artifact.
cp "$COVDIR/merged.cov" "$ROOT/coverage-merged.out"

# Scope filter: drop code outside the meaningful-test bar (see header) — the
# process entrypoint (cmd/server), the zap logger constructor, and the pure no-op
# Metrics sink (NopMetrics, kept in metrics_nop.go) whose empty bodies cannot
# fail. Everything else — every domain rule and adapter — must clear the bar.
EXCLUDE_RE='/cmd/[^/]+/|/internal/config/logger\.go:|/internal/domain/service/metrics_nop\.go:|/internal/adapter/inbound/lifecycle/observer_nop\.go:'
grep -vE "$EXCLUDE_RE" "$COVDIR/merged.cov" > "$COVDIR/scoped.cov"

echo "==> per-package coverage (scoped)"
go tool cover -func="$COVDIR/scoped.cov"

TOTAL="$(go tool cover -func="$COVDIR/scoped.cov" | awk '/^total:/ {gsub("%","",$3); print $3}')"

echo ""
echo "==> combined coverage (scoped): ${TOTAL}%  (threshold ${THRESHOLD}%)"

# Emit the GitHub Actions step summary line when running in CI.
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  printf '**Combined coverage (unit + integration + e2e, scoped):** `%s%%` (gate ≥ %s%%)\n' \
    "$TOTAL" "$THRESHOLD" >> "$GITHUB_STEP_SUMMARY"
fi

# Numeric comparison (awk, so no bc dependency).
if awk -v t="$TOTAL" -v th="$THRESHOLD" 'BEGIN { exit !(t+0 >= th+0) }'; then
  echo "✓ coverage gate passed"
else
  echo "::error::coverage ${TOTAL}% is below the ${THRESHOLD}% gate (SC-008/SC-011)"
  exit 1
fi
