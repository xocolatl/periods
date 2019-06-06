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
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

    /*
     * REFERENCES:
     *     SQL:2016 4.15.2.2
     *     SQL:2016 11.27
     */

    /* Must be a regular persistent base table. SQL:2016 11.27 SR 2 */

    SELECT n.nspname, c.relname, c.relpersistence, c.relkind
    INTO schema_name, table_name, persistence, kind
    FROM pg_class AS c
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
    IF EXISTS (SELECT FROM pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_class, period_name)) THEN
        RAISE EXCEPTION 'a column named system_time already exists for table "%"', table_class;
    END IF;

    /* The standard says that the columns must not exist already, but we don't obey that rule for now. */

    /* Get start column information */
    SELECT a.attnum, a.atttypid, a.attnotnull
    INTO start_attnum, start_type, start_notnull
    FROM pg_attribute AS a
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
    FROM pg_attribute AS a
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
    FROM pg_constraint AS c
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
    FROM pg_constraint AS c
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
            FROM pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (table_class, start_column_name);
        END IF;

        IF end_attnum = 0 THEN
            SELECT a.attnum
            INTO end_attnum
            FROM pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (table_class, end_column_name);
        END IF;
    END IF;

    generated_always_trigger := array_to_string(ARRAY[table_name, 'system_time', 'generated', 'always'], '_');
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION periods.generated_always_as_row_start_end()', generated_always_trigger, table_class);

    write_history_trigger := array_to_string(ARRAY[table_name, 'system_time', 'write', 'history'], '_');
    EXECUTE format(' CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION periods.write_history()', write_history_trigger, table_class);

    INSERT INTO periods.periods (table_name, period_name, start_column_name, end_column_name, range_type, bounds_check_constraint, infinity_check_constraint, generated_always_trigger, write_history_trigger)
    VALUES (table_class, period_name, start_column_name, end_column_name, 'tstzrange', bounds_check_constraint, infinity_check_constraint, generated_always_trigger, write_history_trigger);

    RETURN true;
END;
$function$;

