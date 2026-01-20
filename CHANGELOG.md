# CHANGELOG

All notable changes to this project will be documented in this file.

## Unreleased

### New features

* `singleflight::Group` now supports custom hashers for keys.

## [0.6.1] - 2026-01-11

### New features

* Implement `singleflight` pattern for deduplicating concurrent requests.

## [0.6.0] - 2026-01-04

### Breaking changes

* All channel errors are now unified follow the same `[Try](Send|Recv)Error` pattern. ([#98](https://github.com/fast/mea/pull/98))
* `broadcast::channel` and the related types are moved to one level deeper module `broadcast::overflow`. ([#99](https://github.com/fast/mea/pull/99))

### Improvements

* `oneshot::Sender` and `oneshot::Receiver` now always implement `Debug`.
