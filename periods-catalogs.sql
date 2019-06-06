CREATE TYPE periods.drop_behavior AS ENUM ('CASCADE', 'RESTRICT');
CREATE TYPE periods.fk_actions AS ENUM ('CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION');
CREATE TYPE periods.fk_match_types AS ENUM ('FULL', 'PARTIAL', 'SIMPLE');

/*
 * All referencing columns must be either name or regsomething in order for
 * pg_dump to work properly.  Plain OIDs are not allowed but attribute numbers
 * are, so that we don't have to track renames.
 *
 * Anything declared as regsomething and created for the period (such as the
 * "__as_of" function), should be UNIQUE.  If Postgres already verifies
 * uniqueness, such as constraint names on a table, then we don't need to do it
 * also.
 */

CREATE TABLE periods.periods (
    table_name regclass NOT NULL,
    period_name name NOT NULL,
    start_column_name name NOT NULL,
    end_column_name name NOT NULL,
    range_type regtype NOT NULL,
    bounds_check_constraint name NOT NULL,
    infinity_check_constraint name,
    generated_always_trigger name,
    write_history_trigger name,

    PRIMARY KEY (table_name, period_name),

    CHECK (start_column_name <> end_column_name),
    CHECK (period_name = 'system_time' AND num_nulls(infinity_check_constraint, generated_always_trigger, write_history_trigger) = 0
            OR num_nonnulls(infinity_check_constraint, generated_always_trigger, write_history_trigger) = 0)
);
--SELECT pg_catalog.pg_extension_config_dump('periods.periods', '');

COMMENT ON TABLE periods.periods IS 'The main catalog for periods.  All "DDL" operations for periods must first take an exclusive lock on this table.';

CREATE VIEW periods.information_schema__periods AS
    SELECT current_catalog AS table_catalog,
           n.nspname AS table_schema,
           c.relname AS table_name,
           p.period_name,
           p.start_column_name,
           p. end_column_name
    FROM periods.periods AS p
    JOIN pg_class AS c ON c.oid = p.table_name
    JOIN pg_namespace AS n ON n.oid = c.relnamespace;

CREATE TABLE periods.for_portion_views (
    table_name regclass NOT NULL,
    period_name name NOT NULL,
    view_name regclass NOT NULL,
    trigger_name name NOT NULL,

    PRIMARY KEY (table_name, period_name),

    FOREIGN KEY (table_name, period_name) REFERENCES periods.periods,

    UNIQUE (view_name)
);
--SELECT pg_catalog.pg_extension_config_dump('periods.for_portion_views', '');

CREATE TABLE periods.unique_keys (
    key_name name NOT NULL,
    table_name regclass NOT NULL,
    column_names name[] NOT NULL,
    period_name name NOT NULL,
    unique_constraint name NOT NULL,
    exclude_constraint name NOT NULL,

    PRIMARY KEY (key_name),

    FOREIGN KEY (table_name, period_name) REFERENCES periods.periods
);
--SELECT pg_catalog.pg_extension_config_dump('periods.unique_keys', '');

COMMENT ON TABLE periods.unique_keys IS 'A registry of UNIQUE/PRIMARY keys using periods WITHOUT OVERLAPS';

CREATE TABLE periods.foreign_keys (
    key_name name NOT NULL,
    table_name regclass NOT NULL,
    column_names name[] NOT NULL,
    period_name name NOT NULL,
    unique_key name NOT NULL,
    match_type periods.fk_match_types NOT NULL DEFAULT 'SIMPLE',
    delete_action periods.fk_actions NOT NULL DEFAULT 'NO ACTION',
    update_action periods.fk_actions NOT NULL DEFAULT 'NO ACTION',
    fk_insert_trigger name NOT NULL,
    fk_update_trigger name NOT NULL,
    uk_update_trigger name NOT NULL,
    uk_delete_trigger name NOT NULL,

    PRIMARY KEY (key_name),

    FOREIGN KEY (table_name, period_name) REFERENCES periods.periods,
    FOREIGN KEY (unique_key) REFERENCES periods.unique_keys,

    CHECK (delete_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT')),
    CHECK (update_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT'))
);
--SELECT pg_catalog.pg_extension_config_dump('periods.foreign_keys', '');

COMMENT ON TABLE periods.foreign_keys IS 'A registry of foreign keys using periods WITHOUT OVERLAPS';

CREATE TABLE periods.system_versioning (
    table_name regclass NOT NULL,
    period_name name NOT NULL,
    history_table_name regclass NOT NULL,
    view_name regclass NOT NULL,
    func_as_of regprocedure NOT NULL,
    func_between regprocedure NOT NULL,
    func_between_symmetric regprocedure NOT NULL,
    func_from_to regprocedure NOT NULL,

    PRIMARY KEY (table_name),

    FOREIGN KEY (table_name, period_name) REFERENCES periods.periods,

    CHECK (period_name = 'system_time'),

    UNIQUE (history_table_name),
    UNIQUE (view_name),
    UNIQUE (func_as_of),
    UNIQUE (func_between),
    UNIQUE (func_between_symmetric),
    UNIQUE (func_from_to)
);
--SELECT pg_catalog.pg_extension_config_dump('periods.sysver_registry', '');

COMMENT ON TABLE periods.system_versioning IS 'A registry of tables with SYSTEM VERSIONING';

