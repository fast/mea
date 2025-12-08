# `mea` Project Context for Agents

This file provides context and instructions for AI agents working on the `mea` (Make Easy Async) project.

## Project Overview

`mea` is a runtime-agnostic Rust library providing essential synchronization primitives (Mutex, RwLock, Barrier, etc.) for asynchronous programming. It is designed to work with any async runtime (Tokio, async-std, etc.) and focuses on thread safety (`Send` + `Sync`).

## Key Conventions & Requirements

### 1. Rust Version

- **MSRV:** 1.85.0.
- Ensure code is compatible with this version.

### 2. Build & Test System

The project uses `xtask` to manage build, lint, and test commands. Do **not** run raw `cargo` commands for these tasks if an `xtask` equivalent exists, as `xtask` ensures all flags and environment variables are set correctly.

- **Build:** `cargo x build`
- **Test:** `cargo x test` (Runs tests for the workspace)
- **Lint & Format:** `cargo +nightly x lint` (Runs clippy, rustfmt, taplo, typos, and hawkeye)

### 3. Code Style & Quality

- **Formatting:** Strict adherence to `rustfmt`.
- **Linting:** `clippy` is strictly enforced. No warnings allowed.
- **License Headers:** All files must have the Apache 2.0 license header. The `hawkeye` tool checks this.
- **Typos:** `typos-cli` checks for spelling errors.

### 4. Implementation Details

- **Runtime Agnostic:** Do not depend on `tokio` or `async-std` specific features in the core library logic unless absolutely necessary and behind a feature flag.
- **Safety:** Primitives must be `Send` and `Sync`.
- **Async/Sync:** Focus on async implementations.

## Directory Structure

- `mea/`: The core library crate.
  - `src/`: Source code, organized by primitive (e.g., `mutex/`, `barrier/`).
- `xtask/`: Development tools and task runner.
- `.github/`: CI/CD configurations.

## Workflow for Changes

1.  **Plan:** Understand the goal.
2.  **Modify:** Edit code in `mea/src`.
3.  **Verify:**
    - Run `cargo x test` to ensure correctness.
    - Run `cargo +nightly x lint` to ensure style/lint compliance. **Important:** `lint` can also fix some issues automatically if passed the `--fix` flag (e.g. `cargo +nightly x lint --fix`), but be careful with automatic fixes.

## Common Issues

- **License Header Missing:** If a new file is created, ensure it has the standard license header found in other files.
- **Clippy Warnings:** Fix all clippy warnings before considering a task done.
