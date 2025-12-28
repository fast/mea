# CHANGELOG

All notable changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* All channel errors are now unified follow the same `[Try](Send|Recv)Error` pattern. ([#98](https://github.com/fast/mea/pull/98))
* `broadcast::channel` and the related types are moved to one level deeper module `broadcast::overflow`. ([#99](https://github.com/fast/mea/pull/99))
