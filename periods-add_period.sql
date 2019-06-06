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

    /* Always serialize operations on our catalogs */
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

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
    FROM pg_class AS c
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
    IF EXISTS (SELECT FROM pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_name, period_name)) THEN
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
    FROM pg_attribute AS a
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
    FROM pg_attribute AS a
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
        IF NOT EXISTS (SELECT FROM pg_range AS r WHERE (r.rngtypid, r.rngsubtype, r.rngcollation) = (range_type, start_type, start_collation)) THEN
            RAISE EXCEPTION 'range "%" does not match data type "%"', range_type, start_type;
        END IF;
    ELSE
        SELECT r.rngtypid
        INTO range_type
        FROM pg_range AS r
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
    FROM pg_constraint AS c
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

