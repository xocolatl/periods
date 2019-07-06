-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION periods" to load this file. \quit

/* This extension is non-relocatable */
CREATE SCHEMA periods;

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

    PRIMARY KEY (table_name, period_name),

    CHECK (start_column_name <> end_column_name)
);
SELECT pg_catalog.pg_extension_config_dump('periods.periods', '');

CREATE TABLE periods.system_time_periods (
    table_name regclass NOT NULL,
    period_name name NOT NULL,
    infinity_check_constraint name NOT NULL,
    generated_always_trigger name NOT NULL,
    write_history_trigger name NOT NULL,
    truncate_trigger name NOT NULL,

    PRIMARY KEY (table_name, period_name),
    FOREIGN KEY (table_name, period_name) REFERENCES periods.periods,

    CHECK (period_name = 'system_time')
);
SELECT pg_catalog.pg_extension_config_dump('periods.system_time_periods', '');

COMMENT ON TABLE periods.periods IS 'The main catalog for periods.  All "DDL" operations for periods must first take an exclusive lock on this table.';

CREATE VIEW periods.information_schema__periods AS
    SELECT current_catalog AS table_catalog,
           n.nspname AS table_schema,
           c.relname AS table_name,
           p.period_name,
           p.start_column_name,
           p.end_column_name
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
SELECT pg_catalog.pg_extension_config_dump('periods.for_portion_views', '');

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
SELECT pg_catalog.pg_extension_config_dump('periods.unique_keys', '');

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
SELECT pg_catalog.pg_extension_config_dump('periods.foreign_keys', '');

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
SELECT pg_catalog.pg_extension_config_dump('periods.system_versioning', '');

COMMENT ON TABLE periods.system_versioning IS 'A registry of tables with SYSTEM VERSIONING';


/*
 * These function starting with "_" are private to the periods extension and
 * should not be called by outsiders.  When all the other functions have been
 * translated to C, they will be removed.
 */
CREATE FUNCTION periods._serialize(table_name regclass)
 RETURNS void
 LANGUAGE sql
AS
$function$
/* XXX: Is this the best way to do locking? */
SELECT pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);
$function$;

CREATE FUNCTION periods._choose_name(resizable text[], fixed text DEFAULT NULL, separator text DEFAULT '_', extra integer DEFAULT 2)
 RETURNS name
 IMMUTABLE
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    max_length integer;
    result text;
BEGIN
    /*
     * Reduce the resizable texts until they and the fixed text fit in
     * NAMEDATALEN.  This probably isn't very efficient but it's not on a hot
     * code path so we don't care.
     */

    SELECT max(length(t))
    INTO max_length
    FROM unnest(resizable) AS u (t);

    LOOP
        result := format('%s%s', array_to_string(resizable, separator), separator || fixed);
        IF octet_length(result) <= 63-extra THEN
            RETURN result;
        END IF;

        max_length := max_length - 1;
        resizable := ARRAY (
            SELECT left(t, -1)
            FROM unnest(resizable) WITH ORDINALITY AS u (t, o)
            ORDER BY o
        );
    END LOOP;
END;
$function$;


CREATE FUNCTION periods.add_period(table_name regclass, period_name name, start_column_name name, end_column_name name, range_type regtype DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    kind "char";
    persistence "char";
    bounds_check_constraint name;
    alter_commands text[] DEFAULT '{}';

    start_attnum smallint;
    start_type oid;
    start_collation oid;
    start_notnull boolean;

    end_attnum smallint;
    end_type oid;
    end_collation oid;
    end_notnull boolean;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    IF period_name IS NULL THEN
        RAISE EXCEPTION 'no period name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    /*
     * REFERENCES:
     *     SQL:2016 11.27
     */

    /* Don't allow anything on system versioning history tables (this will be relaxed later) */
    IF EXISTS (SELECT FROM periods.system_versioning AS sv WHERE sv.history_table_name = table_name) THEN
        RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have periods';
    END IF;

    /* Period names are limited to lowercase alphanumeric characters for now */
    period_name := lower(period_name);
    IF period_name !~ '^[a-z_][0-9a-z_]*$' THEN
        RAISE EXCEPTION 'only alphanumeric characters are currently allowed';
    END IF;

    IF period_name = 'system_time' THEN
        RETURN periods.add_system_time_period(table_name, start_column_name, end_column_name);
    END IF;

    /* Must be a regular persistent base table. SQL:2016 11.27 SR 2 */

    SELECT c.relpersistence, c.relkind
    INTO persistence, kind
    FROM pg_catalog.pg_class AS c
    WHERE c.oid = table_name;

    IF kind <> 'r' THEN
        /*
         * The main reason partitioned tables aren't supported yet is simply
         * beceuase I haven't put any thought into it.
         * Maybe it's trivial, maybe not.
         */
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;

        RAISE EXCEPTION 'relation % is not a table', $1;
    END IF;

    IF persistence <> 'p' THEN
        /*
         * We could probably accept unlogged tables but what's the point?
         TODO: in the health check, make sure this remains true
         */
        RAISE EXCEPTION 'table must be persistent';
    END IF;

    /*
     * Check if period already exists.  Actually no other application time
     * periods are allowed per spec, but we don't obey that.  We can have as
     * many application time periods as we want.
     *
     * SQL:2016 11.27 SR 5.b
     */
    IF EXISTS (SELECT FROM periods.periods AS p WHERE (p.table_name, p.period_name) = (table_name, period_name)) THEN
        RAISE EXCEPTION 'period for "%" already exists on table "%"', period_name, table_name;
    END IF;

    /*
     * Although we are not creating a new object, the SQL standard says that
     * periods are in the same namespace as columns, so prevent that.
     *
     * SQL:2016 11.27 SR 5.c
     */
    IF EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE (a.attrelid, a.attname) = (table_name, period_name))
    THEN
        RAISE EXCEPTION 'a column named "%" already exists for table "%"', period_name, table_name;
    END IF;

    /*
     * Contrary to SYSTEM_TIME periods, the columns must exist already for
     * application time periods.
     *
     * SQL:2016 11.27 SR 5.d
     */

    /* Get start column information */
    SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
    INTO start_attnum, start_type, start_collation, start_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_name, start_column_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" not found in table "%"', start_column_name, table_name;
    END IF;

    IF start_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in periods';
    END IF;

    /* Get end column information */
    SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
    INTO end_attnum, end_type, end_collation, end_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_name, end_column_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" not found in table "%"', end_column_name, table_name;
    END IF;

    IF end_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in periods';
    END IF;

    /*
     * Verify compatibility of start/end columns.  The standard says these must
     * be either date or timestamp, but we allow anything with a corresponding
     * range type because why not.
     *
     * SQL:2016 11.27 SR 5.g
     */
    IF start_type <> end_type THEN
        RAISE EXCEPTION 'start and end columns must be of same type';
    END IF;

    IF start_collation <> end_collation THEN
        RAISE EXCEPTION 'start and end columns must be of same collation';
    END IF;

    /* Get the range type that goes with these columns */
    IF range_type IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_range AS r
            WHERE (r.rngtypid, r.rngsubtype, r.rngcollation) = (range_type, start_type, start_collation))
        THEN
            RAISE EXCEPTION 'range "%" does not match data type "%"', range_type, start_type;
        END IF;
    ELSE
        SELECT r.rngtypid
        INTO range_type
        FROM pg_catalog.pg_range AS r
        JOIN pg_opclass AS c ON c.oid = r.rngsubopc
        WHERE (r.rngsubtype, r.rngcollation) = (start_type, start_collation)
          AND c.opcdefault;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'no default range type for %', start_type::regtype;
        END IF;
    END IF;

    /*
     * Period columns must not be nullable.
     *
     * SQL:2016 11.27 SR 5.h
     */
    IF NOT start_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', start_column_name);
    END IF;
    IF NOT end_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', end_column_name);
    END IF;

    /*
     * Find and appropriate a CHECK constraint to make sure that start < end.
     * Create one if necessary.
     *
     * SQL:2016 11.27 GR 2.b
     */
    SELECT c.conname
    INTO bounds_check_constraint
    FROM pg_catalog.pg_constraint AS c
    WHERE c.conrelid = table_name
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', start_column_name, end_column_name);

    IF NOT FOUND THEN
        bounds_check_constraint := table_name || '_' || period_name || '_check';
        alter_commands := alter_commands || format('ADD CONSTRAINT %I CHECK (%I < %I)', bounds_check_constraint, start_column_name, end_column_name);
    END IF;

    /* If we've created any work for ourselves, do it now */
    IF alter_commands <> '{}' THEN
        EXECUTE format('ALTER TABLE %s %s', table_name, array_to_string(alter_commands, ', '));
    END IF;

    INSERT INTO periods.periods (table_name, period_name, start_column_name, end_column_name, range_type, bounds_check_constraint)
    VALUES (table_name, period_name, start_column_name, end_column_name, range_type, bounds_check_constraint);

    RETURN true;
END;
$function$;

CREATE FUNCTION periods.drop_period(table_name regclass, period_name name, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    period_row periods.periods;
    system_time_period_row periods.system_time_periods;
    system_versioning_row periods.system_versioning;
    portion_view regclass;
    is_dropped boolean;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    IF period_name IS NULL THEN
        RAISE EXCEPTION 'no period name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    /*
     * Has the table been dropped already?  This could happen if the period is
     * being dropped by the health_check event trigger or through a DROP CASCADE.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_name);

    SELECT p.*
    INTO period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_name, period_name);

    IF NOT FOUND THEN
        RAISE NOTICE 'period % not found on table %', period_name, table_name;
        RETURN false;
    END IF;

    /* Drop the "for portion" view if it hasn't been dropped already */
    DELETE FROM periods.for_portion_views AS fpv
    WHERE (fpv.table_name, fpv.period_name) = (table_name, period_name)
    RETURNING fpv.view_name INTO portion_view;

    IF FOUND AND EXISTS (
        SELECT FROM pg_catalog.pg_class AS c
        WHERE c.oid = portion_view)
    THEN
        EXECUTE format('DROP VIEW %s %s', portion_view, drop_behavior);
    END IF;

    /* If this is a system_time period, get rid of the triggers */
    DELETE FROM periods.system_time_periods AS stp
    WHERE stp.table_name = table_name
    RETURNING stp.* INTO system_time_period_row;

    IF FOUND AND NOT is_dropped THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_name, system_time_period_row.infinity_check_constraint);
        EXECUTE format('DROP TRIGGER %I ON %s', system_time_period_row.generated_always_trigger, table_name);
        EXECUTE format('DROP TRIGGER %I ON %s', system_time_period_row.write_history_trigger, table_name);
        EXECUTE format('DROP TRIGGER %I ON %s', system_time_period_row.truncate_trigger, table_name);
    END IF;

    IF drop_behavior = 'RESTRICT' THEN
        /* Check for UNIQUE or PRIMARY KEYs */
        IF EXISTS (
            SELECT FROM periods.unique_keys AS uk
            WHERE (uk.table_name, uk.period_name) = (table_name, period_name))
        THEN
            RAISE EXCEPTION 'period % is part of a UNIQUE or PRIMARY KEY', period_name;
        END IF;

        /* Check for FOREIGN KEYs */
        IF EXISTS (
            SELECT FROM periods.foreign_keys AS fk
            WHERE (fk.table_name, fk.period_name) = (table_name, period_name))
        THEN
            RAISE EXCEPTION 'period % is part of a FOREIGN KEY', period_name;
        END IF;

        /* Check for SYSTEM VERSIONING */
        IF EXISTS (
            SELECT FROM periods.system_versioning AS sv
            WHERE (sv.table_name, sv.period_name) = (table_name, period_name))
        THEN
            RAISE EXCEPTION 'table % has SYSTEM VERSIONING', table_name;
        END IF;

        DELETE FROM periods.periods AS p
        WHERE (p.table_name, p.period_name) = (table_name, period_name);

        RETURN true;
    END IF;

    /* We must be in CASCADE mode now */

    PERFORM periods.drop_foreign_key(table_name, fk.key_name)
    FROM periods.foreign_keys AS fk
    WHERE (fk.table_name, fk.period_name) = (table_name, period_name);

    PERFORM periods.drop_unique_key(table_name, uk.key_name, drop_behavior, purge)
    FROM periods.unique_keys AS uk
    WHERE (uk.table_name, uk.period_name) = (table_name, period_name);

    /*
     * Save ourselves the NOTICE if this table doesn't have SYSTEM
     * VERSIONING.
     *
     * We don't do like above because the purge is different.  We don't want
     * dropping SYSTEM VERSIONING to drop our infinity constraint; only
     * dropping the PERIOD should do that.
     */
    IF EXISTS (
        SELECT FROM periods.system_versioning AS sv
        WHERE (sv.table_name, sv.period_name) = (table_name, period_name))
    THEN
        PERFORM periods.drop_system_versioning(table_name, drop_behavior, purge);
    END IF;

    IF NOT is_dropped AND purge THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',
            table_name, period_row.bounds_check_constraint);
    END IF;

    DELETE FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_name, period_name);

    RETURN true;
END;
$function$;

CREATE FUNCTION periods.add_system_time_period(table_class regclass, start_column_name name DEFAULT 'system_time_start', end_column_name name DEFAULT 'system_time_end')
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    period_name CONSTANT name := 'system_time';

    schema_name name;
    table_name name;
    kind "char";
    persistence "char";
    bounds_check_constraint name;
    infinity_check_constraint name;
    generated_always_trigger name;
    write_history_trigger name;
    truncate_trigger name;
    alter_commands text[] DEFAULT '{}';

    start_attnum smallint;
    start_type oid;
    start_collation oid;
    start_notnull boolean;

    end_attnum smallint;
    end_type oid;
    end_collation oid;
    end_notnull boolean;
BEGIN
    IF table_class IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_class);

    /*
     * REFERENCES:
     *     SQL:2016 4.15.2.2
     *     SQL:2016 11.27
     */

    /* Must be a regular persistent base table. SQL:2016 11.27 SR 2 */

    SELECT n.nspname, c.relname, c.relpersistence, c.relkind
    INTO schema_name, table_name, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_class;

    IF kind <> 'r' THEN
        /*
         * The main reason partitioned tables aren't supported yet is simply
         * beceuase I haven't put any thought into it.
         * Maybe it's trivial, maybe not.
         */
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;

        RAISE EXCEPTION 'relation % is not a table', $1;
    END IF;

    IF persistence <> 'p' THEN
        /*
         * We could probably accept unlogged tables but what's the point?
         TODO: in the health check, make sure this remains true
         */
        RAISE EXCEPTION 'table must be persistent';
    END IF;

    /*
     * Check if period already exists.
     *
     * SQL:2016 11.27 SR 4.a
     */
    IF EXISTS (SELECT FROM periods.periods AS p WHERE (p.table_name, p.period_name) = (table_class, period_name)) THEN
        RAISE EXCEPTION 'period for SYSTEM_TIME already exists on table "%"', table_class;
    END IF;

    /*
     * Although we are not creating a new object, the SQL standard says that
     * periods are in the same namespace as columns, so prevent that.
     *
     * SQL:2016 11.27 SR 4.b
     */
    IF EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_class, period_name)) THEN
        RAISE EXCEPTION 'a column named system_time already exists for table "%"', table_class;
    END IF;

    /* The standard says that the columns must not exist already, but we don't obey that rule for now. */

    /* Get start column information */
    SELECT a.attnum, a.atttypid, a.attnotnull
    INTO start_attnum, start_type, start_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_class, start_column_name);

    IF NOT FOUND THEN
       /*
        * First add the column with DEFAULT of -infinity to fill the
        * current rows, then replace the DEFAULT with transaction_timestamp() for future
        * rows.
        *
        * The default value is just for self-documentation anyway because
        * the trigger will enforce the value.
        */
        alter_commands := alter_commands || format('ADD COLUMN %I timestamp with time zone NOT NULL DEFAULT ''-infinity''', start_column_name);

        start_attnum := 0;
        start_type := 'timestamp with time zone'::regtype;
        start_notnull := true;
    END IF;
    alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT transaction_timestamp()', start_column_name);

    IF start_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in periods';
    END IF;

    /* Get end column information */
    SELECT a.attnum, a.atttypid, a.attnotnull
    INTO end_attnum, end_type, end_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_class, end_column_name);

    IF NOT FOUND THEN
        alter_commands := alter_commands || format('ADD COLUMN %I timestamp with time zone NOT NULL DEFAULT ''infinity''', end_column_name);

        end_attnum := 0;
        end_type := 'timestamp with time zone'::regtype;
        end_notnull := true;
    ELSE
        alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT ''infinity''', end_column_name);
    END IF;

    IF end_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in periods';
    END IF;

    /* Verify compatibility of start/end columns */
    IF start_type <> 'timestamp with time zone'::regtype OR end_type <> 'timestamp with time zone'::regtype THEN
        RAISE EXCEPTION 'start and end columns must be of type "timestamp with time zone"';
    END IF;

    /* can't be part of a foreign key */
    IF EXISTS (
        SELECT FROM periods.foreign_keys AS fk
        WHERE fk.table_name = table_class
          AND fk.column_names && ARRAY[start_column_name, end_column_name])
    THEN
        RAISE EXCEPTION 'columns for SYSTEM_TIME must not be part of foreign keys';
    END IF;

    /*
     * Period columns must not be nullable.
     */
    IF NOT start_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', start_column_name);
    END IF;
    IF NOT end_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', end_column_name);
    END IF;

    /*
     * Find and appropriate a CHECK constraint to make sure that start < end.
     * Create one if necessary.
     *
     * SQL:2016 11.27 GR 2.b
     */
    SELECT c.conname
    INTO bounds_check_constraint
    FROM pg_catalog.pg_constraint AS c
    WHERE c.conrelid = table_class
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', start_column_name, end_column_name);

    IF NOT FOUND THEN
        bounds_check_constraint := table_name || '_' || period_name || '_check';
        alter_commands := alter_commands || format('ADD CONSTRAINT %I CHECK (%I < %I)', bounds_check_constraint, start_column_name, end_column_name);
    END IF;

    /*
     * Find and appropriate a CHECK constraint to make sure that end = 'infinity'.
     * Create one if necessary.
     *
     * SQL:2016 4.15.2.2
     */
    SELECT c.conname
    INTO infinity_check_constraint
    FROM pg_catalog.pg_constraint AS c
    WHERE c.conrelid = table_class
      AND c.contype = 'c'
      AND pg_get_constraintdef(c.oid) = format('CHECK ((%I = ''infinity''::timestamp with time zone))', end_column_name);

    IF NOT FOUND THEN
        infinity_check_constraint := array_to_string(ARRAY[table_name, end_column_name, 'infinity', 'check'], '_');
        alter_commands := alter_commands || format('ADD CONSTRAINT %I CHECK (%I = ''infinity''::timestamp with time zone)', infinity_check_constraint, end_column_name);
    END IF;

    /* If we've created any work for ourselves, do it now */
    IF alter_commands <> '{}' THEN
        EXECUTE format('ALTER TABLE %I.%I %s', schema_name, table_name, array_to_string(alter_commands, ', '));

        IF start_attnum = 0 THEN
            SELECT a.attnum
            INTO start_attnum
            FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (table_class, start_column_name);
        END IF;

        IF end_attnum = 0 THEN
            SELECT a.attnum
            INTO end_attnum
            FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (table_class, end_column_name);
        END IF;
    END IF;

    generated_always_trigger := array_to_string(ARRAY[table_name, 'system_time', 'generated', 'always'], '_');
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE periods.generated_always_as_row_start_end()', generated_always_trigger, table_class);

    write_history_trigger := array_to_string(ARRAY[table_name, 'system_time', 'write', 'history'], '_');
    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE periods.write_history()', write_history_trigger, table_class);

    truncate_trigger := array_to_string(ARRAY[table_name, 'truncate'], '_');
    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE periods.truncate_system_versioning()', truncate_trigger, table_class);

    INSERT INTO periods.periods (table_name, period_name, start_column_name, end_column_name, range_type, bounds_check_constraint)
    VALUES (table_class, period_name, start_column_name, end_column_name, 'tstzrange', bounds_check_constraint);

    INSERT INTO periods.system_time_periods (table_name, period_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger)
    VALUES (table_class, period_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger);

    RETURN true;
END;
$function$;

CREATE FUNCTION periods.drop_system_time_period(table_name regclass, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE sql
AS
$function$
SELECT periods.drop_period(table_name, 'system_time', drop_behavior, purge);
$function$;

CREATE FUNCTION periods.generated_always_as_row_start_end()
 RETURNS trigger
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME';

CREATE FUNCTION periods.write_history()
 RETURNS trigger
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME';

CREATE FUNCTION periods.truncate_system_versioning()
 RETURNS trigger
 LANGUAGE plpgsql
 STRICT
AS
$function$
#variable_conflict use_variable
DECLARE
    history_table_name name;
BEGIN
    SELECT sv.history_table_name
    INTO history_table_name
    FROM periods.system_versioning AS sv
    WHERE sv.table_name = TG_RELID;

    IF FOUND THEN
        EXECUTE format('TRUNCATE %s', history_table_name);
    END IF;

    RETURN NULL;
END;
$function$;

/*
CREATE FUNCTION periods.add_portion_views(table_name regclass DEFAULT NULL, period_name name DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    view_name name;
BEGIN
    /*
     * If table_name and period_name are specified, then just add the views for that.
     *
     * If no period is specified, add the views for all periods of the table.
     *
     * If no table is specified, add the views everywhere.
     *
     * If no table is specified but a period is, that doesn't make any sense.
     */
    IF table_name IS NULL AND period_name IS NOT NULL THEN
        RAISE EXCEPTION 'cannot specify period name without table name';
    END IF;

    /* Can't use FOR PORTION OF on SYSTEM_TIME columns */
    IF period_name = 'system_time' THEN
        RAISE EXCEPTION 'cannot use FOR PORTION OF on SYSTEM_TIME periods';
    END IF;

    FOR r IN
        SELECT n.nspname AS schema_name, c.relname AS table_name, p.period_name
        FROM periods.periods AS p
        JOIN pg_class AS c ON c.oid = p.table_name
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE (table_name IS NULL OR p.table_name = table_name)
          AND (period_name IS NULL OR p.period_name = period_name)
          AND p.period_name <> 'system_time'
          AND NOT EXISTS (
                SELECT FROM periods.for_portion_views AS _fpv
                WHERE (_fpv.table_name, _fpv.period_name) = (p.table_name, p.period_name))
    LOOP
        /* TODO: make sure these names fit NAMEDATALEN */
        view_name := r.table_name || '__for_portion_of_' || r.period_name;
        trigger_name := 'for_portion_of_' || r.period_name;
        EXECUTE format('CREATE VIEW %1$I.%2$I AS TABLE %1$I.%3$I', r.schema_name, view_name, r.table_name);
        EXECUTE format('CREATE TRIGGER %I INSTEAD OF UPDATE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE periods.update_portion_of(%s, %I)'
            trigger_name, r.schema_name, r.table_name, r.period_name);
        INSERT INTO periods.for_portion_views (table_name, period_name, view_name, trigger_name)
            VALUES (format('%I.%I', r.schema_name, r.table_name), r.period_name, format('%I.%I', r.schema_name, view_name));
    END LOOP;

    RETURN true;
END;
$function$;

CREATE FUNCTION periods.drop_portion_views(table_name regclass, period_name name, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
BEGIN
    RETURN true;
END;
$function$;

CREATE FUNCTION periods.update_portion_of()
 RETURNS trigger
 LANGAUGE plpgsql
AS
$function$
#variable_conflict use_variable
BEGIN
    --TODO
    RETURN NEW;
END;
$function$;
*/


CREATE FUNCTION periods.add_unique_key(
        table_name regclass,
        column_names name[],
        period_name name,
        key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    period_row periods.periods;
    column_attnums smallint[];
    period_attnums smallint[];
    idx integer;
    constraint_record record;
    pass integer;
    sql text;
    alter_cmds text[];
    unique_index regclass;
    exclude_index regclass;
    unique_sql text;
    exclude_sql text;
    unique_exists boolean DEFAULT false;
    exclude_exists boolean DEFAULT false;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    SELECT p.*
    INTO period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_name, period_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'period "%" does not exist', period_name;
    END IF;

    /* For convenience, put the period's attnums in an array */
    period_attnums := ARRAY[
        (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (period_row.table_name, period_row.start_column_name)),
        (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (period_row.table_name, period_row.end_column_name))
    ];

    /* Get attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_attribute AS a ON (a.attrelid, a.attname) = (table_name, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', column_names[idx];
    END IF;

    /* Make sure the period columns aren't also in the normal columns */
    IF period_row.start_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', period_row.start_column_name;
    END IF;
    IF period_row.end_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', period_row.end_column_name;
    END IF;

    /* If we were given a unique constraint to use, look it up and make sure it matches */
    unique_sql := 'UNIQUE (' || array_to_string(column_names || period_row.start_column_name || period_row.end_column_name, ', ') || ')';
    IF unique_constraint IS NOT NULL THEN
        SELECT c.oid, c.contype, c.condeferrable, c.conkey
        INTO constraint_record
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.conname) = (table_name, unique_constraint);

        IF NOT FOUND THEN
            RAISE EXCEPTION 'constraint "%" does not exist', unique_constraint;
        END IF;

        IF constraint_record.contype NOT IN ('p', 'u') THEN
            RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY or UNIQUE KEY', unique_constraint;
        END IF;

        IF constraint_record.condeferrable THEN
            /* SQL:2016 TODO */
            RAISE EXCEPTION 'constraint "%" must not be DEFERRABLE', unique_constraint;
        END IF;

        IF NOT constraint_record.conkey = column_attnums || period_attnums THEN
            RAISE EXCEPTION 'constraint "%" does not match', unique_constraint;
        END IF;

        /* Looks good, let's use it. */
    END IF;

    /*
     * If we were given an exclude constraint to use, look it up and make sure
     * it matches.  We do that by generating the text that we expect
     * pg_get_constraintdef() to output and compare against that instead of
     * trying to deal with the internally stored components like we did for the
     * UNIQUE constraint.
     *
     * We will use this same text to create the constraint if it doesn't exist.
     */
    DECLARE
        withs text[];
        len integer;
    BEGIN
        len := array_length(column_attnums, 1);

        SELECT array_agg(format('%I WITH =', column_name) ORDER BY n.ordinality)
        INTO withs
        FROM unnest(column_names) WITH ORDINALITY AS n (column_name, ordinality);

        withs := withs || format('%I(%I, %I, ''[)''::text) WITH &&',
            period_row.range_type, period_row.start_column_name, period_row.end_column_name);

        exclude_sql := format('EXCLUDE USING gist (%s)', array_to_string(withs, ', '));
    END;

    IF exclude_constraint IS NOT NULL THEN
        SELECT c.oid, c.contype, c.condeferrable, pg_get_constraintdef(c.oid) AS definition
        INTO constraint_record
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.conname) = (table_name, exclude_constraint);

        IF NOT FOUND THEN
            RAISE EXCEPTION 'constraint "%" does not exist', exclude_constraint;
        END IF;

        IF constraint_record.contype <> 'x' THEN
            RAISE EXCEPTION 'constraint "%" is not an EXCLUDE constraint', exclude_constraint;
        END IF;

        IF constraint_record.condeferrable THEN
            /* SQL:2016 TODO */
            RAISE EXCEPTION 'constraint "%" must not be DEFERRABLE', exclude_constraint;
        END IF;

        IF constraint_record.definition <> exclude_sql THEN
            RAISE EXCEPTION 'constraint "%" does not match', exclude_constraint;
        END IF;

        /* Looks good, let's use it. */
    END IF;

    /*
     * Generate a name for the unique constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF key_name IS NULL THEN
        key_name := periods._choose_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = table_name)]
                || column_names
                || ARRAY[period_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM periods.unique_keys AS uk
       WHERE uk.key_name = key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    key_name := key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* Time to make the underlying constraints */
    alter_cmds := '{}';
    IF unique_constraint IS NULL THEN
        alter_cmds := alter_cmds || ('ADD ' || unique_sql);
    END IF;

    IF exclude_constraint IS NULL THEN
        alter_cmds := alter_cmds || ('ADD ' || exclude_sql);
    END IF;

    IF alter_cmds <> '{}' THEN
        SELECT format('ALTER TABLE %I.%I %s', n.nspname, c.relname, array_to_string(alter_cmds, ', '))
        INTO sql
        FROM pg_catalog.pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_name;

        EXECUTE sql;
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL THEN
        SELECT c.conname, c.conindid
        INTO unique_constraint, unique_index
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_name, 'u')
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    /* If we don't already have an exclude_constraint, it must be the one with the highest oid */
    IF exclude_constraint IS NULL THEN
        SELECT c.conname, c.conindid
        INTO exclude_constraint, exclude_index
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_name, 'x')
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    INSERT INTO periods.unique_keys (key_name, table_name, column_names, period_name, unique_constraint, exclude_constraint)
    VALUES (key_name, table_name, column_names, period_name, unique_constraint, exclude_constraint);

    RETURN true;
END;
$function$;

CREATE FUNCTION periods.drop_unique_key(table_name regclass, key_name name, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row periods.foreign_keys;
    unique_key_row periods.unique_keys;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    FOR unique_key_row IN
        SELECT uk.*
        FROM periods.unique_keys AS uk
        WHERE uk.table_name = table_name
          AND (uk.key_name = key_name OR key_name IS NULL)
    LOOP
        /* Cascade to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.key_name
            FROM periods.foreign_keys AS fk
            WHERE fk.unique_key = unique_key_row.key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.key_name, foreign_key_row.key_name, foreign_key_row.table_name;
            END IF;

            PERFORM drop_foreign_key(NULL, foreign_key_row.key_name);
        END LOOP;

        DELETE FROM periods.unique_keys AS uk
        WHERE uk.key_name = unique_key_row.key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF purge AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            WHERE c.oid = unique_key_row.table_name)
        THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, DROP CONSTRAINT %I',
                unique_key_row.table_name, unique_key_row.unique_constraint, unique_key_row.exclude_constraint);
        END IF;
    END LOOP;
END;
$function$;

CREATE FUNCTION periods.uk_update_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    jold jsonb;
BEGIN
    /*
     * This function is called when a table referenced by foreign keys with
     * periods is updated.  It checks to verify that the referenced table still
     * contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     *
     * If this is a NO ACTION constraint, we need to check if there is a new
     * row that still satisfies the constraint, in which case there is no
     * error.
     */

    /* Use jsonb to look up values by parameterized names */
    jold := row_to_json(OLD);

    /* Check the constraint */
    PERFORM periods.validate_foreign_key_old_row(TG_ARGV[0], jold, true);

    RETURN NULL;
END;
$function$;

CREATE FUNCTION periods.uk_delete_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    jold jsonb;
BEGIN
    /*
     * This function is called when a table referenced by foreign keys with
     * periods is deleted from.  It checks to verify that the referenced table
     * still contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     *
     * The only difference between NO ACTION and RESTRICT is when the check is
     * done, so this function is used for both.
     */

    /* Use jsonb to look up values by parameterized names */
    jold := row_to_json(OLD);

    /* Check the constraint */
    PERFORM periods.validate_foreign_key_old_row(TG_ARGV[0], jold, false);

    RETURN NULL;
END;
$function$;


CREATE FUNCTION periods.add_foreign_key(
        table_name regclass,
        column_names name[],
        period_name name,
        ref_unique_name name,
        match_type periods.fk_match_types DEFAULT 'SIMPLE',
        update_action periods.fk_actions DEFAULT 'NO ACTION',
        delete_action periods.fk_actions DEFAULT 'NO ACTION',
        key_name name DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    period_row periods.periods;
    ref_period_row periods.periods;
    unique_row periods.unique_keys;
    column_attnums smallint[];
    idx integer;
    pass integer;
    upd_action text DEFAULT '';
    del_action text DEFAULT '';
    fk_insert_name name;
    fk_update_name name;
    uk_update_name name;
    uk_delete_name name;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    /* Get the period involved */
    SELECT p.*
    INTO period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_name, period_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'period "%" does not exist', period_name;
    END IF;

    IF period_row.period_name = 'system_time' THEN
        RAISE EXCEPTION 'periods for SYSTEM_TIME may not appear in foreign keys';
    END IF;

    /* Get column attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_attribute AS a ON (a.attrelid, a.attname) = (table_name, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', column_names[idx];
    END IF;

    /* Make sure the period columns aren't also in the normal columns */
    IF period_row.start_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', period_row.start_column_name;
    END IF;
    IF period_row.end_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', period_row.end_column_name;
    END IF;

    /* Columns can't be part of any SYSTEM_TIME period */
    IF EXISTS (
        SELECT FROM periods.periods AS p
        WHERE (p.table_name, p.period_name) = (table_name, 'system_time')
          AND ARRAY[p.start_column_name, p.end_column_name] && column_names)
    THEN
        RAISE EXCEPTION 'columns for SYSTEM_TIME must not be part of foreign keys';
    END IF;

    /* Get the unique key we're linking to */
    SELECT uk.*
    INTO unique_row
    FROM periods.unique_keys AS uk
    WHERE uk.key_name = ref_unique_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key "%" does not exist', ref_unique_name;
    END IF;

    /* Get the unique key's period */
    SELECT p.*
    INTO ref_period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (unique_row.table_name, unique_row.period_name);

    IF period_row.range_type <> ref_period_row.range_type THEN
        RAISE EXCEPTION 'period types "%" and "%" are incompatible',
            period_row.period_name, ref_period_row.period_name;
    END IF;

    /* Check that all the columns match */
    IF EXISTS (
        SELECT FROM unnest(column_names, unique_row.column_names) AS u (fk_attname, uk_attname)
        JOIN pg_attribute AS fa ON (fa.attrelid, fa.attname) = (table_name, u.fk_attname)
        JOIN pg_attribute AS ua ON (ua.attrelid, ua.attname) = (unique_row.table_name, u.uk_attname)
        WHERE (fa.atttypid, fa.atttypmod, fa.attcollation) <> (ua.atttypid, ua.atttypmod, ua.attcollation))
    THEN
        RAISE EXCEPTION 'column types do not match';
    END IF;

    /* The range types must match, too */
    IF period_row.range_type <> ref_period_row.range_type THEN
        RAISE EXCEPTION 'period types do not match';
    END IF;

    /*
     * Generate a name for the foreign constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF key_name IS NOT NULL THEN
        key_name := periods._choose_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = table_name)]
               || column_names
               || ARRAY[period_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM periods.foreign_keys AS fk
       WHERE fk.key_name = key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    key_name := key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* See if we're deferring the constraints or not */
    IF update_action = 'NO ACTION' THEN
        upd_action := ' DEFERRABLE INITIALLY DEFERRED';
    END IF;
    IF delete_action = 'NO ACTION' THEN
        del_action := ' DEFERRABLE INITIALLY DEFERRED';
    END IF;

    /* Time to make the underlying triggers */
    fk_insert_name := periods._choose_name(ARRAY[key_name], 'fk_insert');
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER INSERT ON %s FROM %s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE periods.fk_insert_check(%L)',
        fk_insert_name, table_name, unique_row.table_name, key_name);
    fk_update_name := periods._choose_name(ARRAY[key_name], 'fk_update');
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER UPDATE ON %s FROM %s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE periods.fk_update_check(%L)',
        fk_update_name, table_name, unique_row.table_name, key_name);
    uk_update_name := periods._choose_name(ARRAY[key_name], 'uk_update');
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER UPDATE ON %s FROM %s%s FOR EACH ROW EXECUTE PROCEDURE periods.uk_update_check(%L)',
        uk_update_name, unique_row.table_name, table_name, upd_action, key_name);
    uk_delete_name := periods._choose_name(ARRAY[key_name], 'uk_delete');
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER DELETE ON %s FROM %s%s FOR EACH ROW EXECUTE PROCEDURE periods.uk_delete_check(%L)',
        uk_delete_name, unique_row.table_name, table_name, del_action, key_name);

    INSERT INTO periods.foreign_keys (key_name, table_name, column_names, period_name, unique_key, match_type, update_action, delete_action,
                                      fk_insert_trigger, fk_update_trigger, uk_update_trigger, uk_delete_trigger)
    VALUES (key_name, table_name, column_names, period_name, unique_row.key_name, match_type, update_action, delete_action,
            fk_insert_name, fk_update_name, uk_update_name, uk_delete_name);

    /* Validate the constraint on existing data */
    PERFORM periods.validate_foreign_key_new_row(key_name, NULL);

    RETURN key_name;
END;
$function$;

CREATE FUNCTION periods.drop_foreign_key(table_name regclass, key_name name)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row periods.foreign_keys;
BEGIN
    IF table_name IS NULL AND key_name IS NULL THEN
        RAISE EXCEPTION 'no table or key name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    FOR foreign_key_row IN
        SELECT fk.*
        FROM periods.foreign_keys AS fk
        WHERE (fk.table_name = table_name OR table_name IS NULL)
          AND (fk.key_name = key_name OR key_name IS NULL)
    LOOP
        DELETE FROM periods.foreign_keys AS fk
        WHERE fk.key_name = foreign_key_row.key_name;

        /* Make sure the table hasn't been dropped before doing these. */
        IF EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            WHERE c.oid = foreign_key_row.table_name)
        THEN
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_insert_trigger, foreign_key_row.table_name);
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_update_trigger, foreign_key_row.table_name);
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_update_trigger, foreign_key_row.table_name);
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_delete_trigger, foreign_key_row.table_name);
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;

CREATE FUNCTION periods.fk_insert_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    jnew jsonb;
BEGIN
    /*
     * This function is called when a new row is inserted into a table
     * containing foreign keys with periods.  It checks to verify that the
     * referenced table contains the proper data to satisfy the foreign key
     * constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     */

    /* Use jsonb to look up values by parameterized names */
    jnew := row_to_json(NEW);

    /* Check the constraint */
    PERFORM periods.validate_foreign_key_new_row(TG_ARGV[0], jnew);

    RETURN NULL;
END;
$function$;

CREATE FUNCTION periods.fk_update_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    jnew jsonb;
BEGIN
    /*
     * This function is called when a table containing foreign keys with
     * periods is updated.  It checks to verify that the referenced table
     * contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     */

    /* Use jsonb to look up values by parameterized names */
    jnew := row_to_json(NEW);

    /* Check the constraint */
    PERFORM periods.validate_foreign_key_new_row(TG_ARGV[0], jnew);

    RETURN NULL;
END;
$function$;

/*
 * This function either returns true or raises an exception.
 */
CREATE FUNCTION periods.validate_foreign_key_old_row(foreign_key_name name, row_data jsonb, is_update boolean)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_info record;
    column_name name;
    has_nulls boolean;
    uk_column_names text[];
    uk_column_values text[];
    fk_column_names text;
    violation boolean;
    still_matches boolean;

    QSQL CONSTANT text := 
        'SELECT EXISTS ( '
        '    SELECT FROM %1$I.%2$I AS t '
        '    WHERE ROW(%3$s) = ROW(%6$s) '
        '      AND t.%4$I <= %7$L '
        '      AND t.%5$I >= %8$L '
        '%9$s'
        ')';

BEGIN
    SELECT fc.oid AS fk_table_oid,
           fn.nspname AS fk_schema_name,
           fc.relname AS fk_table_name,
           fk.column_names AS fk_column_names,
           fp.period_name AS fk_period_name,
           fp.start_column_name AS fk_start_column_name,
           fp.end_column_name AS fk_end_column_name,

           uc.oid AS uk_table_oid,
           un.nspname AS uk_schema_name,
           uc.relname AS uk_table_name,
           uk.column_names AS uk_column_names,
           up.period_name AS uk_period_name,
           up.start_column_name AS uk_start_column_name,
           up.end_column_name AS uk_end_column_name,

           fk.match_type,
           fk.update_action,
           fk.delete_action
    INTO foreign_key_info
    FROM periods.foreign_keys AS fk
    JOIN periods.periods AS fp ON (fp.table_name, fp.period_name) = (fk.table_name, fk.period_name)
    JOIN pg_class AS fc ON fc.oid = fk.table_name
    JOIN pg_namespace AS fn ON fn.oid = fc.relnamespace
    JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
    JOIN periods.periods AS up ON (up.table_name, up.period_name) = (uk.table_name, uk.period_name)
    JOIN pg_class AS uc ON uc.oid = uk.table_name
    JOIN pg_namespace AS un ON un.oid = uc.relnamespace
    WHERE fk.key_name = foreign_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'foreign key "%" not found', foreign_key_name;
    END IF;

    FOREACH column_name IN ARRAY foreign_key_info.uk_column_names LOOP
        IF row_data->>column_name IS NULL THEN
            /*
             * If the deleted row had nulls in the referenced columns then
             * there was no possible referencing row (until we implement
             * PARTIAL) so we can just stop here.
             */
            RETURN true;
        END IF;
        uk_column_names := uk_column_names || ('t.' || quote_ident(column_name));
        uk_column_values := uk_column_values || quote_literal(row_data->>column_name);
    END LOOP;

    IF is_update AND foreign_key_info.update_action = 'NO ACTION' THEN
        EXECUTE format(QSQL, foreign_key_info.uk_schema_name,
                             foreign_key_info.uk_table_name,
                             array_to_string(uk_column_names, ', '),
                             foreign_key_info.uk_start_column_name,
                             foreign_key_info.uk_end_column_name,
                             array_to_string(uk_column_values, ', '),
                             row_data->>foreign_key_info.uk_start_column_name,
                             row_data->>foreign_key_info.uk_end_column_name,
                             'FOR KEY SHARE')
        INTO still_matches;

        IF still_matches THEN
            RETURN true;
        END IF;
    END IF;

    SELECT string_agg('t.' || quote_ident(u.c), ', ' ORDER BY u.ordinality)
    INTO fk_column_names
    FROM unnest(foreign_key_info.fk_column_names) WITH ORDINALITY AS u (c, ordinality);

    EXECUTE format(QSQL, foreign_key_info.fk_schema_name,
                         foreign_key_info.fk_table_name,
                         fk_column_names,
                         foreign_key_info.fk_start_column_name,
                         foreign_key_info.fk_end_column_name,
                         array_to_string(uk_column_values, ', '),
                         row_data->>foreign_key_info.uk_start_column_name,
                         row_data->>foreign_key_info.uk_end_column_name,
                         '')
    INTO violation;

    IF violation THEN
        RAISE EXCEPTION 'update or delete on table "%" violates foreign key constraint "%" on table "%"',
            foreign_key_info.uk_table_oid::regclass,
            foreign_key_name,
            foreign_key_info.fk_table_oid::regclass;
    END IF;

    RETURN true;
END;
$function$;

/*
 * This function either returns true or raises an exception.
 */
CREATE FUNCTION periods.validate_foreign_key_new_row(foreign_key_name name, row_data jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_info record;
    row_clause text DEFAULT 'true';
    violation boolean;

	QSQL CONSTANT text :=
        'SELECT EXISTS ( '
        '    SELECT FROM %9$I.%10$I AS fk '
        '    WHERE NOT EXISTS ( '
        '        SELECT FROM (SELECT uk.uk_start_value, '
        '                            uk.uk_end_value, '
        '                            nullif(lag(uk.uk_end_value) OVER (ORDER BY uk.uk_start_value), uk.uk_start_value) AS x '
        '                     FROM (SELECT uk.%5$I AS uk_start_value, '
        '                                  uk.%7$I AS uk_end_value '
        '                           FROM %1$I.%2$I AS uk '
        '                           WHERE ROW(%3$s) = ROW(%11$s) '
        '                             AND uk.%5$I <= fk.%15$I '
        '                             AND uk.%7$I >= fk.%13$I '
        '                           FOR KEY SHARE '
        '                          ) AS uk '
        '                    ) AS uk '
        '        WHERE uk.uk_start_value < fk.%15$I '
        '          AND uk.uk_end_value >= fk.%13$I '
        '        HAVING min(uk.uk_start_value) <= fk.%13$I '
        '           AND max(uk.uk_end_value) >= fk.%15$I '
        '           AND array_agg(uk.x) FILTER (WHERE uk.x IS NOT NULL) IS NULL '
        '    ) AND %17$s '
        ')';

BEGIN
    SELECT fc.oid AS fk_table_oid,
           fn.nspname AS fk_schema_name,
           fc.relname AS fk_table_name,
           fk.column_names AS fk_column_names,
           fp.period_name AS fk_period_name,
           fp.start_column_name AS fk_start_column_name,
           fp.end_column_name AS fk_end_column_name,

           un.nspname AS uk_schema_name,
           uc.relname AS uk_table_name,
           uk.column_names AS uk_column_names,
           up.period_name AS uk_period_name,
           up.start_column_name AS uk_start_column_name,
           up.end_column_name AS uk_end_column_name,

           fk.match_type,
           fk.update_action,
           fk.delete_action
    INTO foreign_key_info
    FROM periods.foreign_keys AS fk
    JOIN periods.periods AS fp ON (fp.table_name, fp.period_name) = (fk.table_name, fk.period_name)
    JOIN pg_class AS fc ON fc.oid = fk.table_name
    JOIN pg_namespace AS fn ON fn.oid = fc.relnamespace
    JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
    JOIN periods.periods AS up ON (up.table_name, up.period_name) = (uk.table_name, uk.period_name)
    JOIN pg_class AS uc ON uc.oid = uk.table_name
    JOIN pg_namespace AS un ON un.oid = uc.relnamespace
    WHERE fk.key_name = foreign_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'foreign key "%" not found', foreign_key_name;
    END IF;

    /*
     * Now that we have all of our names, we can see if there are any nulls in
     * the row we were given (if we were given one).
     */
    IF row_data IS NOT NULL THEN
        DECLARE
            column_name name;
            has_nulls boolean;
            all_nulls boolean;
            cols text[] DEFAULT '{}';
            vals text[] DEFAULT '{}';
        BEGIN
            FOREACH column_name IN ARRAY foreign_key_info.fk_column_names LOOP
                has_nulls := has_nulls OR row_data->>column_name IS NULL;
                all_nulls := all_nulls IS NOT false AND row_data->>column_name IS NULL;
                cols := cols || ('fk.' || quote_ident(column_name));
                vals := vals || quote_literal(row_data->>column_name);
            END LOOP;

            IF all_nulls THEN
                /*
                 * If there are no values at all, all three types pass.
                 *
                 * Period columns are by definition NOT NULL so the FULL MATCH
                 * type is only concerned with the non-period columns of the
                 * constraint.  SQL:2016 4.23.3.3
                 */
                RETURN true;
            END IF;

            IF has_nulls THEN
                CASE foreign_key_info.match_type
                    WHEN 'SIMPLE' THEN
                        RETURN true;
                    WHEN 'PARTIAL' THEN
                        RAISE EXCEPTION 'partial not implemented';
                    WHEN 'FULL' THEN
                        RAISE EXCEPTION 'foreign key violated (nulls in FULL)';
                END CASE;
            END IF;

            row_clause := format(' (%s) = (%s)', array_to_string(cols, ', '), array_to_string(vals, ', '));
        END;
    END IF;

    EXECUTE format(QSQL, foreign_key_info.uk_schema_name,
                         foreign_key_info.uk_table_name,
                         array_to_string(foreign_key_info.uk_column_names, ', '),
                         NULL,
                         foreign_key_info.uk_start_column_name,
                         NULL,
                         foreign_key_info.uk_end_column_name,
                         NULL,
                         foreign_key_info.fk_schema_name,
                         foreign_key_info.fk_table_name,
                         array_to_string(foreign_key_info.fk_column_names, ', '),
                         NULL,
                         foreign_key_info.fk_start_column_name,
                         NULL,
                         foreign_key_info.fk_end_column_name,
                         NULL,
                         row_clause)
    INTO violation;

    IF violation THEN
        IF row_data IS NULL THEN
            RAISE EXCEPTION 'foreign key violated by some row';
        ELSE
            RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%"',
                foreign_key_info.fk_table_oid::regclass,
                foreign_key_name;
        END IF;
    END IF;

    RETURN true;
END;
$function$;


CREATE FUNCTION periods.add_system_versioning(table_class regclass)
 RETURNS void
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    schema_name name;
    table_name name;
    history_table_name name;
    view_name name;
    function_as_of_name name;
    function_between_name name;
    function_between_symmetric_name name;
    function_from_to_name name;
    persistence "char";
    kind "char";
    period_row periods.periods;
    history_table_id oid;
BEGIN
    IF table_class IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_class);

    /*
     * REFERENCES:
     *     SQL:2016 4.15.2.2
     *     SQL:2016 11.3 SR 2.3
     *     SQL:2016 11.3 GR 1.c
     *     SQL:2016 11.29
     */

    /* Already registered? SQL:2016 11.29 SR 5 */
    IF EXISTS (SELECT FROM periods.system_versioning AS r WHERE r.table_name = table_class) THEN
        RAISE EXCEPTION 'table already has SYSTEM VERSIONING';
    END IF;

    /* Must be a regular persistent base table. SQL:2016 11.29 SR 2 */

    SELECT n.nspname, c.relname, c.relpersistence, c.relkind
    INTO schema_name, table_name, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_class;

    IF kind <> 'r' THEN
        /*
         * The main reason partitioned tables aren't supported yet is simply
         * beceuase I haven't put any thought into it.
         * Maybe it's trivial, maybe not.
         */
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;

        RAISE EXCEPTION 'relation % is not a table', $1;
    END IF;

    IF persistence <> 'p' THEN
        /*
         * We could probably accept unlogged tables if the history table is
         * also unlogged, but what's the point?
         TODO: in the health check, make sure this remains true
         */
        RAISE EXCEPTION 'table must be persistent';
    END IF;

    /* We need a SYSTEM_TIME period. SQL:2016 11.29 SR 4 */
    SELECT p.*
    INTO period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_class, 'system_time');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'no period for SYSTEM_TIME found for table %', table_class;
    END IF;

    /* Get all of our "fake" infrastructure ready */
    history_table_name := periods._choose_name(ARRAY[table_name], 'history');
    view_name := periods._choose_name(ARRAY[table_name], 'with_history');
    function_as_of_name := periods._choose_name(ARRAY[table_name], '_as_of');
    function_between_name := periods._choose_name(ARRAY[table_name], '_between');
    function_between_symmetric_name := periods._choose_name(ARRAY[table_name], '_between_symmetric');
    function_from_to_name := periods._choose_name(ARRAY[table_name], '_from_to');

    /*
     * Create the history table.  If it already exists we check that all the
     * columns match but otherwise we trust the user.  Perhaps the history
     * table was disconnected in order to change the schema (a case which is
     * not defined by the SQL standard).  Or perhaps the user wanted to
     * partition the history table.
     *
     * There shouldn't be any concurrency issues here because our main catalog
     * is locked.
     */
    SELECT c.oid
    INTO history_table_id
    FROM pg_catalog.pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE (n.nspname, c.relname) = (schema_name, history_table_name);

    IF FOUND THEN
        /* Don't allow any periods on the system table (this will be relaxed later) */
        IF EXISTS (SELECT FROM periods.periods AS p WHERE p.table_name = history_table_id) THEN
            RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have periods';
        END IF;

        /*
         * The query to the attributes is harder than one would think because
         * we need to account for dropped columns.  Basically what we're
         * looking for is that all columns have the same order, name, type, and
         * collation.
         */
        IF EXISTS (
            WITH
            L (attnum, attname, atttypid, atttypmod, attcollation) AS (
                SELECT row_number() OVER (ORDER BY a.attnum),
                       a.attname, a.atttypid, a.atttypmod, a.attcollation
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = table_class
                  AND NOT a.attisdropped
            ),
            R (attnum, attname, atttypid, atttypmod, attcollation) AS (
                SELECT row_number() OVER (ORDER BY a.attnum),
                       a.attname, a.atttypid, a.atttypmod, a.attcollation
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = history_table_id
                  AND NOT a.attisdropped
            )
            SELECT FROM L NATURAL FULL JOIN R
            WHERE L.attnum IS NULL OR R.attnum IS NULL)
        THEN
            RAISE EXCEPTION 'base table "%" and history table "%" are not compatible',
                table_class, history_table_id::regclass;
        END IF;
    ELSE
        EXECUTE format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', schema_name, history_table_name, table_name);
        history_table_id := format('%I.%I', schema_name, history_table_name)::regclass;
        RAISE NOTICE 'history table "%" created for "%", be sure to index it properly',
            history_table_id::regclass, table_class;
    END IF;

    /* Create the "with history" view.  This one we do want to error out on if it exists. */
    EXECUTE format(
        'CREATE VIEW %1$I.%2$I AS TABLE %1$I.%3$I UNION ALL TABLE %1$I.%4$I',
        schema_name, view_name, table_name, history_table_name);

    /*
     * Create functions to simulate the system versioned grammar.  These must
     * be inlinable for any kind of performance.
     */
    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE %4$I <= $1 AND %5$I > $1'
        $$, schema_name, function_as_of_name, view_name, period_row.start_column_name, period_row.end_column_name);

    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE $1 <= $2 AND %5$I > $1 AND %4$I <= $2'
        $$, schema_name, function_between_name, view_name, period_row.start_column_name, period_row.end_column_name);

    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE %5$I > least($1, $2) AND %4$I <= greatest($1, $2)'
        $$, schema_name, function_between_symmetric_name, view_name, period_row.start_column_name, period_row.end_column_name);

    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE $1 < $2 AND %5$I > $1 AND %4$I < $2'
        $$, schema_name, function_from_to_name, view_name, period_row.start_column_name, period_row.end_column_name);

    /* Register it */
    INSERT INTO periods.system_versioning (table_name, period_name, history_table_name, view_name,
                                           func_as_of, func_between, func_between_symmetric, func_from_to)
    VALUES (
        table_class,
        'system_time',
        format('%I.%I', schema_name, history_table_name),
        format('%I.%I', schema_name, view_name),
        format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name)::regprocedure,
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name)::regprocedure,
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name)::regprocedure,
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)::regprocedure
    );
END;
$function$;

CREATE FUNCTION periods.drop_system_versioning(table_name regclass, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    system_versioning_row periods.system_versioning;
    is_dropped boolean;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    /*
     * REFERENCES:
     *     SQL:2016 4.15.2.2
     *     SQL:2016 11.3 SR 2.3
     *     SQL:2016 11.3 GR 1.c
     *     SQL:2016 11.30
     */

    /* We need to delete our row first so that the health check doesn't block us. */
    DELETE FROM periods.system_versioning AS sv
    WHERE sv.table_name = table_name
    RETURNING * INTO system_versioning_row;

    IF NOT FOUND THEN
        RAISE NOTICE 'table % does not have SYSTEM VERSIONING', table_name;
        RETURN false;
    END IF;

    /*
     * Has the table been dropped?  If so, everything else is also dropped
     * except for the history table.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_name);

    IF NOT is_dropped THEN
        /* Drop the functions. */
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of::regprocedure, drop_behavior);
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between::regprocedure, drop_behavior);
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric::regprocedure, drop_behavior);
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to::regprocedure, drop_behavior);

        /* Drop the "with_history" view. */
        EXECUTE format('DROP VIEW %s %s', system_versioning_row.view_name, drop_behavior);
    END IF;

    /*
     * SQL:2016 11.30 GR 2 says "Every row of T that corresponds to a
     * historical system row is effectively deleted at the end of the SQL-
     * statement." but we leave the history table intact in case the user
     * merely wants to make some DDL changes and hook things back up again.
     *
     * The purge parameter tells us that the user really wants to get rid of it
     * all.
     */
    IF NOT is_dropped AND purge THEN
        PERFORM periods.drop_period(table_name, 'system_time', drop_behavior, purge);
        EXECUTE format('DROP TABLE %s %s', system_versioning_row.history_table_name, drop_behavior);
    END IF;

    RETURN true;
END;
$function$;


CREATE FUNCTION periods.health_check()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_name regclass;
    period_name name;
BEGIN
    /* TODO: Lots and lots of stuff missing here */

    /* If one of our tables is being dropped, remove references to it */
    FOR table_name, period_name IN
        SELECT p.table_name, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_event_trigger_dropped_objects() AS dobj ON dobj.objid = p.table_name
        WHERE dobj.object_type = 'table'
    LOOP
        PERFORM periods.drop_period(table_name, period_name, 'CASCADE', true);
    END LOOP;

    /*
     * If a column belonging to one of our periods is dropped, we need to reject that.
     * SQL:2016 11.23 SR 6
     */
    FOR r in
        SELECT dobj.object_identity, p.period_name
        FROM periods.periods AS p
        JOIN pg_attribute AS sa ON (sa.attrelid, sa.attname) = (p.table_name, p.start_column_name)
        JOIN pg_attribute AS ea ON (ea.attrelid, ea.attname) = (p.table_name, p.end_column_name)
        JOIN pg_catalog.pg_event_trigger_dropped_objects() AS dobj ON dobj.objid = p.table_name AND dobj.objsubid IN (sa.attnum, ea.attnum)
        WHERE dobj.object_type = 'table column'
        ORDER BY dobj.original DESC
    LOOP
        RAISE EXCEPTION 'cannot drop column "%" because it is part of the period "%"', r.object_identity, r.period_name;
    END LOOP;

/*

TODO:

-   Don't allow new unique indexes to use a system_time period column. 11.7 SR 5b

*/
END;
$function$;

CREATE EVENT TRIGGER periods_health_check ON sql_drop EXECUTE PROCEDURE periods.health_check();
