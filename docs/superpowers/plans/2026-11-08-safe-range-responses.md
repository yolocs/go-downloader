# Safe Range Responses Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Complete TRACKING.md's next task: handle `416` and `200` on Range safely without stitching mismatched bodies.

**Architecture:** Preserve the README's fail-fast choice once parallel reading starts; switching sources mid-stream could mix representations. Recognize an empty-object probe, avoid reading ignored/error probe bodies, and validate each successful partial response against the requested offsets and probed total before publishing bytes.

**Tech Stack:** Go 1.27, standard library, httptest.

**Spec:** README.md, Correctness invariants and Chunk retries and timeouts; TRACKING.md, Production hardening.

## Global Constraints

- Preserve the public API and retry policy; protocol errors are permanent.
- Do not restart a parallel stream with a plain GET.
- Keep ETag consistency, memory caps, and general correctness fuzzing as separate backlog items.

## Review Focus

- Empty objects: accept only `416` with `bytes */0`, then use the existing single-stream path.
- Wrong same-length ranges: reject missing, malformed, shifted, or inconsistent Content-Range before reading the body.
- Mid-stream `200` or `416`: return an error without appending that response or restarting the stream.
- Stalled ignored/error response bodies: close without draining and cancel sibling workers on terminal errors.
- Retry transport fixtures: provide valid partial-response metadata so timeout/oversize tests continue testing their intended branches.

## Task 1: Probe and chunk response validation

**Files:** probe.go (parse and validate Content-Range, empty probe), reader.go (validate chunk headers, explicit 416), range_response_test.go (regressions), retry_test.go and retry_transport_test.go (valid response fixtures), README.md and TRACKING.md (document behavior).

**Interfaces:** Keep `probe`, `Open`, and `doRangeGetOnce` unchanged. Replace `parseContentRangeTotal` with `parseContentRange(h string) (start, end, total int64, ok bool)` for satisfied and unsatisfied ranges; use -1 offsets for unsatisfied ranges and require a known numeric total.

- [x] Run the baseline suite using the official Go 1.27 toolchain.
- [x] Write integration tests for empty/malformed probes, mismatched 206 chunks, ignored/error probe bodies, and mid-stream terminal responses; run them and confirm the missing behavior fails.
- [x] Implement strict range parsing, probe validation, no probe draining, chunk validation, and explicit permanent 416 rejection.
- [x] Keep existing retry tests meaningful by supplying valid Content-Range headers in timeout and oversized-body fixtures.
- [x] Verify targeted tests, full suite with race detector, vet, and gofmt.
- [x] Document the fail-fast policy and strike through the backlog item after verification.
- [x] Review the change and commit on the task branch.

## Verification record

Baseline suite passed on main. Before implementation, the regressions exposed
successful output of wrong same-length chunks, rejection of empty objects,
and reads of ignored/error probe bodies. The complete suite passed after
implementation; `go test -race -count=1 ./...` passed on Go 1.27.3.
Independent read-only review found no actionable issues. Unchanged-size
representation changes, fallback size changes, and memory limits remain
separate tracking items.
