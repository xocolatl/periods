# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

  - Add support for excluded columns. These are columns for which
    updates do not cause `GENERATED ALWAYS AS ROW START` to change, and
    historical rows will not be generated.

    This is not in the standard, but was requested by several people.

## [1.0] – 2019-08-25

### Added

  - Initial release. Supports all features of the SQL Standard
    concerning periods and `SYSTEM VERSIONING`.

[Unreleased]: https://github.com/xocolatl/periods/compare/v1.0...HEAD
[1.0]: https://github.com/xocolatl/periods/releases/tag/v1.0
