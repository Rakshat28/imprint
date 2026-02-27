# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-02-28

### Added
- Implemented metadata preservation (xattrs, permissions, timestamps) during the deduplication process (#25).

### Fixed
- Implemented atomic vault renaming to prevent master file corruption during unexpected interruptions (#29).
- Fixed a critical safety issue to ensure the master file is fully restored if a subsequent reflink operation fails (#27).
- Fixed and improved the hardlink fallback logic for filesystems that do not support CoW reflinks (#24).

### Changed
- Hardened the integration test suite to strictly validate sparse files, bit-rot simulations, and CI runner isolation (#30).