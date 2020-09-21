# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
### Fixed

## [1.2] – 2020-09-21

### Added

  - Add Access Control to prevent users from modifying the history.  Only the table owner
    and superusers can do this because we can't prevent it.

  - Compatibility with PostgreSQL 13

### Fixed

  - Use SPI to insert into the history table.  They previous way of doing it didn't
    update the indexes, leading to wrong results depending on the execution plan.

    Users must REINDEX all indexes on history tables.

  - Ensure all of our functions are `SECURITY DEFINER`.

  - Ensure ownership of history and for-portion objects follow the main table's owner.

  - Quote all identifiers when building queries.

  - Don't use `regprocedure` in our catalogs, they prevent `pg_upgrade` from working.
    This reduces functionality a little but, but not being able to upgrade is a
    showstopper.

## [1.1] – 2020-02-05

### Added

  - Add support for excluded columns. These are columns for which
    updates do not cause `GENERATED ALWAYS AS ROW START` to change, and
    historical rows will not be generated.

    This is not in the standard, but was requested by several people.

  - Cache some query plans in the C code.

  - Describe the proper way to `ALTER` a table with `SYSTEM VERSIONING`.

### Fixed

  - Match columns in the main table and the history table by name.  This was an
    issue if either of the tables had dropped columns.

  - Use the main table's tuple descriptor when there is no mapping necessary with the
    history table's tuple descriptor (see previous item).  This works around PostgreSQL
    bug #16242 where missing attributes are not considered when detecting differences.

## [1.0] – 2019-08-25

### Added

  - Initial release. Supports all features of the SQL Standard
    concerning periods and `SYSTEM VERSIONING`.

[Unreleased]: https://github.com/xocolatl/periods/compare/v1.2...HEAD
[1.2]: https://github.com/xocolatl/periods/compare/v1.1...v1.2
[1.1]: https://github.com/xocolatl/periods/compare/v1.0...v1.1
[1.0]: https://github.com/xocolatl/periods/releases/tag/v1.0
