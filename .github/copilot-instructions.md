# GitHub Copilot Instructions for fjall (structured-world fork)

## Project Overview

This is a **maintained fork** of [fjall-rs/fjall](https://github.com/fjall-rs/fjall) — a log-structured, embeddable key-value storage engine in Rust. We maintain additional features for the [CoordiNode](https://github.com/structured-world/coordinode) database engine while contributing patches upstream.

**Dependency:** fjall depends on [lsm-tree](https://github.com/structured-world/lsm-tree) (also our fork).

## Review Scope Rules (CRITICAL)

**Review ONLY code within the PR's diff.** For issues found in code outside the diff:
- Do NOT suggest inline code fixes for unchanged lines
- Instead, suggest creating a **separate issue** with the finding (e.g., "Consider opening an issue to validate buffer size before allocation — this is outside the scope of this PR")

**Each PR has a defined scope in its description.** Read the "out of scope" section before reviewing. If something is listed as out of scope, do not flag it — it is tracked in another PR.

**Cross-PR awareness:** This fork has multiple feature branches in parallel. If a fix or feature seems missing, check whether it exists in another open PR before suggesting it. Reference the other PR number if known.

**Prefer issue suggestions over code suggestions for out-of-scope findings.** This keeps PRs focused and reviewable.

## Rust Code Standards

- **Unsafe code:** Prefer safe alternatives. `unsafe` requires `// SAFETY:` comment.
- **Error handling:** No `unwrap()` or `expect()` on I/O paths. `expect()` is acceptable for lock poisoning with `#[expect(clippy::expect_used, reason = "...")]`.
- **Clippy:** Code must pass `cargo clippy --all-features -- -D warnings`. Use `#[expect(...)]` (not `#[allow(...)]`) for justified suppressions.
- **Casts:** Prefer `TryFrom`/`TryInto`. `as` casts need `#[expect(clippy::cast_possible_truncation)]` with reason.
- **Feature gates:** Code behind `#[cfg(feature = "...")]` must compile with any feature combination.

## Testing Standards

- **No mocks for storage:** Tests use real on-disk files via `tempfile::tempdir()`.
- **Test naming:** `fn <what>_<condition>_<expected>()`.
- **Integration tests:** In `tests/` directory, each file is a separate test binary.

## Commit Message Format

```
<type>(scope): <description>

- Detail 1
- Detail 2
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `style`, `chore`, `perf`, `ci`, `build`, `revert`

**Forbidden patterns:** "address review", "fix PR comments", "WIP", "temporary"

## Build and Test

```bash
cargo clippy --all-features -- -D warnings  # Lint (strict)
cargo test                                   # All tests
cargo test --features __internal_whitebox    # Whitebox tests
cargo fmt --all -- --check                   # Format check
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `lz4` | LZ4 compression (default) |
| `zstd` | Zstd compression (forwarded to lsm-tree) |
| `bytes_1` | Use `bytes` crate for value types |
| `metrics` | Expose prometheus metrics |
| `__internal_whitebox` | Internal testing utilities |

## Architecture Notes

- `src/db.rs` — Database handle (opening, persistence, configuration)
- `src/keyspace/` — Keyspace management (column family equivalent)
- `src/journal/` — Write-ahead journal (WAL)
- `src/compaction/` — Background compaction coordination
- `src/supervisor.rs` — Background task manager (flush, compaction)
- `src/snapshot_tracker.rs` — MVCC snapshot tracking
- `src/batch/` — Atomic write batches
- Transactional modes: `OptimisticTxDatabase`, `SingleWriterTxDatabase`
