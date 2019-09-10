ALTER TABLE periods.system_time_periods
    ADD COLUMN excluded_column_names name[] NOT NULL DEFAULT '{}';

DROP FUNCTION periods.add_system_time_period(regclass, name, name, name, name, name, name, name);

CREATE FUNCTION periods.add_system_time_period(
    table_class regclass,
    start_column_name name DEFAULT 'system_time_start',
    end_column_name name DEFAULT 'system_time_end',
    bounds_check_constraint name DEFAULT NULL,
    infinity_check_constraint name DEFAULT NULL,
    generated_always_trigger name DEFAULT NULL,
    write_history_trigger name DEFAULT NULL,
    truncate_trigger name DEFAULT NULL,
    excluded_column_names name[] DEFAULT '{}')
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
    alter_commands text[] DEFAULT '{}';

    start_attnum smallint;
    start_type oid;
    start_collation oid;
    start_notnull boolean;

    end_attnum smallint;
    end_type oid;
    end_collation oid;
    end_notnull boolean;

    DATE_OID CONSTANT integer := 1082;
    TIMESTAMP_OID CONSTANT integer := 1114;
    TIMESTAMPTZ_OID CONSTANT integer := 1184;
    range_type regtype;
BEGIN
    IF table_class IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_class);

    /*
     * REFERENCES:
     *     SQL:2016 4.15.2.2
     *     SQL:2016 11.7
     *     SQL:2016 11.27
     */

    /* The columns must not be part of UNIQUE keys. SQL:2016 11.7 SR 5)b) */
    IF EXISTS (
        SELECT FROM periods.unique_keys AS uk
        WHERE uk.column_names && ARRAY[start_column_name, end_column_name])
    THEN
        RAISE EXCEPTION 'columns in period for SYSTEM_TIME are not allowed in UNIQUE keys';
    END IF;

    /* Must be a regular persistent base table. SQL:2016 11.27 SR 2 */

    SELECT n.nspname, c.relname, c.relpersistence, c.relkind
    INTO schema_name, table_name, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
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
        /* We could probably accept unlogged tables but what's the point? */
        RAISE EXCEPTION 'table "%" must be persistent', table_class;
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
    IF start_type::regtype NOT IN ('date', 'timestamp without time zone', 'timestamp with time zone') THEN
        RAISE EXCEPTION 'SYSTEM_TIME periods must be of type "date", "timestamp without time zone", or "timestamp with time zone"';
    END IF;
    IF start_type <> end_type THEN
        RAISE EXCEPTION 'start and end columns must be of same type';
    END IF;

    /* Get appropriate range type */
    CASE start_type
        WHEN DATE_OID THEN range_type := 'daterange';
        WHEN TIMESTAMP_OID THEN range_type := 'tsrange';
        WHEN TIMESTAMPTZ_OID THEN range_type := 'tstzrange';
    ELSE
        RAISE EXCEPTION 'unexpected data type: "%"', start_type::regtype;
    END CASE;

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
    DECLARE
        condef CONSTANT text := format('CHECK ((%I < %I))', start_column_name, end_column_name);
        context text;
    BEGIN
        IF bounds_check_constraint IS NOT NULL THEN
            /* We were given a name, does it exist? */
            SELECT pg_catalog.pg_get_constraintdef(c.oid)
            INTO context
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_class, bounds_check_constraint)
              AND c.contype = 'c';

            IF FOUND THEN
                /* Does it match? */
                IF context <> condef THEN
                    RAISE EXCEPTION 'constraint "%" on table "%" does not match', bounds_check_constraint, table_class;
                END IF;
            ELSE
                /* If it doesn't exist, we'll use the name for the one we create. */
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
            END IF;
        ELSE
            /* No name given, can we appropriate one? */
            SELECT c.conname
            INTO bounds_check_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = table_class
              AND c.contype = 'c'
              AND pg_catalog.pg_get_constraintdef(c.oid) = condef;

            /* Make our own then */
            IF NOT FOUND THEN
                SELECT c.relname
                INTO table_name
                FROM pg_catalog.pg_class AS c
                WHERE c.oid = table_class;

                bounds_check_constraint := periods._choose_name(ARRAY[table_name, period_name], 'check');
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
            END IF;
        END IF;
    END;

    /*
     * Find and appropriate a CHECK constraint to make sure that end = 'infinity'.
     * Create one if necessary.
     *
     * SQL:2016 4.15.2.2
     */
    DECLARE
        condef CONSTANT text := format('CHECK ((%I = ''infinity''::timestamp with time zone))', end_column_name);
        context text;
    BEGIN
        IF infinity_check_constraint IS NOT NULL THEN
            /* We were given a name, does it exist? */
            SELECT pg_catalog.pg_get_constraintdef(c.oid)
            INTO context
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_class, infinity_check_constraint)
              AND c.contype = 'c';

            IF FOUND THEN
                /* Does it match? */
                IF context <> condef THEN
                    RAISE EXCEPTION 'constraint "%" on table "%" does not match', infinity_check_constraint, table_class;
                END IF;
            ELSE
                /* If it doesn't exist, we'll use the name for the one we create. */
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
            END IF;
        ELSE
            /* No name given, can we appropriate one? */
            SELECT c.conname
            INTO infinity_check_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = table_class
              AND c.contype = 'c'
              AND pg_catalog.pg_get_constraintdef(c.oid) = condef;

            /* Make our own then */
            IF NOT FOUND THEN
                SELECT c.relname
                INTO table_name
                FROM pg_catalog.pg_class AS c
                WHERE c.oid = table_class;

                infinity_check_constraint := periods._choose_name(ARRAY[table_name, end_column_name], 'infinity_check');
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
            END IF;
        END IF;
    END;

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

    generated_always_trigger := coalesce(
        generated_always_trigger,
        periods._choose_name(ARRAY[table_name], 'system_time_generated_always'));
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE periods.generated_always_as_row_start_end()', generated_always_trigger, table_class);

    write_history_trigger := coalesce(
        write_history_trigger,
        periods._choose_name(ARRAY[table_name], 'system_time_write_history'));
    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE periods.write_history()', write_history_trigger, table_class);

    truncate_trigger := coalesce(
        truncate_trigger,
        periods._choose_name(ARRAY[table_name], 'truncate'));
    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE periods.truncate_system_versioning()', truncate_trigger, table_class);

    INSERT INTO periods.periods (table_name, period_name, start_column_name, end_column_name, range_type, bounds_check_constraint)
    VALUES (table_class, period_name, start_column_name, end_column_name, range_type, bounds_check_constraint);

    INSERT INTO periods.system_time_periods (
        table_name, period_name, infinity_check_constraint,
        generated_always_trigger, write_history_trigger, truncate_trigger,
        excluded_column_names)
    VALUES (
        table_class, period_name, infinity_check_constraint,
        generated_always_trigger, write_history_trigger, truncate_trigger,
        excluded_column_names);

    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION periods.drop_protection()
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
    /*
     * This function is called after the fact, so we have to just look to see
     * if anything is missing in the catalogs if we just store the name and not
     * a reg* type.
     */

    ---
    --- periods
    ---

    /* If one of our tables is being dropped, remove references to it */
    FOR table_name, period_name IN
        SELECT p.table_name, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.table_name
        WHERE dobj.object_type = 'table'
        ORDER BY dobj.ordinality
    LOOP
        PERFORM periods.drop_period(table_name, period_name, 'CASCADE', true);
    END LOOP;

    /*
     * If a column belonging to one of our periods is dropped, we need to reject that.
     * SQL:2016 11.23 SR 6
     */
    FOR r IN
        SELECT dobj.object_identity, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_attribute AS sa ON (sa.attrelid, sa.attname) = (p.table_name, p.start_column_name)
        JOIN pg_catalog.pg_attribute AS ea ON (ea.attrelid, ea.attname) = (p.table_name, p.end_column_name)
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.table_name AND dobj.objsubid IN (sa.attnum, ea.attnum)
        WHERE dobj.object_type = 'table column'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop column "%" because it is part of the period "%"',
            r.object_identity, r.period_name;
    END LOOP;

    /* Also reject dropping the rangetype */
    FOR r IN
        SELECT dobj.object_identity, p.table_name, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.range_type
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop rangetype "%" because it is used in period "%" on table "%"',
            r.object_identity, r.period_name, r.table_name;
    END LOOP;

    ---
    --- system_time_periods
    ---

    /* Complain if the infinity CHECK constraint is missing. */
    FOR r IN
        SELECT p.table_name, p.infinity_check_constraint
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (p.table_name, p.infinity_check_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in SYSTEM_TIME period',
            r.infinity_check_constraint, r.table_name;
    END LOOP;

    /* Complain if the GENERATED ALWAYS AS ROW START/END trigger is missing. */
    FOR r IN
        SELECT p.table_name, p.generated_always_trigger
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (p.table_name, p.generated_always_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
            r.generated_always_trigger, r.table_name;
    END LOOP;

    /* Complain if the write_history trigger is missing. */
    FOR r IN
        SELECT p.table_name, p.write_history_trigger
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (p.table_name, p.write_history_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
            r.write_history_trigger, r.table_name;
    END LOOP;

    /* Complain if the TRUNCATE trigger is missing. */
    FOR r IN
        SELECT p.table_name, p.truncate_trigger
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (p.table_name, p.truncate_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
            r.truncate_trigger, r.table_name;
    END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
    FOR r IN
        SELECT stp.table_name, u.column_name
        FROM periods.system_time_periods AS stp
        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from SYSTEM VERSIONING',
            r.column_name, r.table_name;
    END LOOP;

    ---
    --- for_portion_views
    ---

    /* Reject dropping the FOR PORTION OF view. */
    FOR r IN
        SELECT dobj.object_identity
        FROM periods.for_portion_views AS fpv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = fpv.view_name
        WHERE dobj.object_type = 'view'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop view "%", call "periods.drop_for_portion_view()" instead',
            r.object_identity;
    END LOOP;

    /* Complain if the FOR PORTION OF trigger is missing. */
    FOR r IN
        SELECT fpv.table_name, fpv.period_name, fpv.view_name, fpv.trigger_name
        FROM periods.for_portion_views AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fpv.view_name, fpv.trigger_name))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on view "%" because it is used in FOR PORTION OF view for period "%" on table "%"',
            r.trigger_name, r.view_name, r.period_name, r.table_name;
    END LOOP;

    /* Complain if the table's primary key has been dropped. */
    FOR r IN
        SELECT fpv.table_name, fpv.period_name
        FROM periods.for_portion_views AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (fpv.table_name, 'p'))
    LOOP
        RAISE EXCEPTION 'cannot drop primary key on table "%" because it has a FOR PORTION OF view for period "%"',
            r.table_name, r.period_name;
    END LOOP;

    ---
    --- unique_keys
    ---

    /*
     * We don't need to protect the individual columns as long as we protect
     * the indexes.  PostgreSQL will make sure they stick around.
     */

    /* Complain if the indexes implementing our unique indexes are missing. */
    FOR r IN
        SELECT uk.key_name, uk.table_name, uk.unique_constraint
        FROM periods.unique_keys AS uk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (uk.table_name, uk.unique_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in period unique key "%"',
            r.unique_constraint, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT uk.key_name, uk.table_name, uk.exclude_constraint
        FROM periods.unique_keys AS uk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (uk.table_name, uk.exclude_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in period unique key "%"',
            r.exclude_constraint, r.table_name, r.key_name;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /* Complain if any of the triggers are missing */
    FOR r IN
        SELECT fk.key_name, fk.table_name, fk.fk_insert_trigger
        FROM periods.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_name, fk.fk_insert_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.fk_insert_trigger, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT fk.key_name, fk.table_name, fk.fk_update_trigger
        FROM periods.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_name, fk.fk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.fk_update_trigger, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT fk.key_name, uk.table_name, fk.uk_update_trigger
        FROM periods.foreign_keys AS fk
        JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_name, fk.uk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.uk_update_trigger, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT fk.key_name, uk.table_name, fk.uk_delete_trigger
        FROM periods.foreign_keys AS fk
        JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_name, fk.uk_delete_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.uk_delete_trigger, r.table_name, r.key_name;
    END LOOP;

    ---
    --- system_versioning
    ---

    FOR r IN
        SELECT dobj.object_identity, sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = sv.history_table_name
        WHERE dobj.object_type = 'table'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop table "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.object_identity, r.table_name;
    END LOOP;

    FOR r IN
        SELECT dobj.object_identity, sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = sv.view_name
        WHERE dobj.object_type = 'view'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop view "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.object_identity, r.table_name;
    END LOOP;

    FOR r IN
        SELECT dobj.object_identity, sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid IN (sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to)
        WHERE dobj.object_type = 'function'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop function "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.object_identity, r.table_name;
    END LOOP;
END;
$function$;

CREATE OR REPLACE FUNCTION periods.rename_following()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    sql text;
BEGIN
    /*
     * Anything that is stored by reg* type will auto-adjust, but anything we
     * store by name will need to be updated after a rename. One way to do this
     * is to recreate the constraints we have and pull new names out that way.
     * If we are unable to do something like that, we must raise an exception.
     */

    ---
    --- periods
    ---

    /*
     * Start and end columns of a period can be found by the bounds check
     * constraint.
     */
    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.periods SET start_column_name = %L, end_column_name = %L WHERE (table_name, period_name) = (%L::regclass, %L)',
            sa.attname, ea.attname, p.table_name, p.period_name)
        FROM periods.periods AS p
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (p.table_name, p.bounds_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = p.table_name
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = p.table_name
        WHERE (p.start_column_name, p.end_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Inversely, the bounds check constraint can be retrieved via the start
     * and end columns.
     */
    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.periods SET bounds_check_constraint = %L WHERE (table_name, period_name) = (%L::regclass, %L)',
            c.conname, p.table_name, p.period_name)
        FROM periods.periods AS p
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = p.table_name
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = p.table_name
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = p.table_name
        WHERE p.bounds_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
          AND (p.start_column_name, p.end_column_name) = (sa.attname, ea.attname)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (p.table_name, p.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- system_time_periods
    ---

    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.system_time_periods SET infinity_check_constraint = %L WHERE table_name = %L::regclass',
            c.conname, p.table_name)
        FROM periods.periods AS p
        JOIN periods.system_time_periods AS stp ON (stp.table_name, stp.period_name) = (p.table_name, p.period_name)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = p.table_name
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = p.table_name
        WHERE stp.infinity_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I = ''infinity''::%s))', ea.attname, format_type(ea.atttypid, ea.atttypmod))
          AND p.end_column_name = ea.attname
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (stp.table_name, stp.infinity_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.system_time_periods SET generated_always_trigger = %L WHERE table_name = %L::regclass',
            t.tgname, stp.table_name)
        FROM periods.system_time_periods AS stp
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
        WHERE t.tgname <> stp.generated_always_trigger
          AND t.tgfoid = 'periods.generated_always_as_row_start_end()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.generated_always_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.system_time_periods SET write_history_trigger = %L WHERE table_name = %L::regclass',
            t.tgname, stp.table_name)
        FROM periods.system_time_periods AS stp
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
        WHERE t.tgname <> stp.write_history_trigger
          AND t.tgfoid = 'periods.write_history()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.write_history_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.system_time_periods SET truncate_trigger = %L WHERE table_name = %L::regclass',
            t.tgname, stp.table_name)
        FROM periods.system_time_periods AS stp
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
        WHERE t.tgname <> stp.truncate_trigger
          AND t.tgfoid = 'periods.truncate_system_versioning()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.truncate_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
    FOR r IN
        SELECT stp.table_name, u.column_name
        FROM periods.system_time_periods AS stp
        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from SYSTEM VERSIONING',
            r.column_name, r.table_name;
    END LOOP;

    ---
    --- for_portion_views
    ---

    FOR sql IN
        SELECT pg_catalog.format('UPDATE periods.for_portion_views SET trigger_name = %L WHERE (table_name, period_name) = (%L::regclass, %L)',
            t.tgname, fpv.table_name, fpv.period_name)
        FROM periods.for_portion_views AS fpv
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = fpv.view_name
        WHERE t.tgname <> fpv.trigger_name
          AND t.tgfoid = 'periods.update_portion_of()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (fpv.table_name, fpv.trigger_name))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- unique_keys
    ---

    FOR sql IN
        SELECT format('UPDATE periods.unique_keys SET column_names = %L WHERE key_name = %L',
            a.column_names, uk.key_name)
        FROM periods.unique_keys AS uk
        JOIN periods.periods AS p ON (p.table_name, p.period_name) = (uk.table_name, uk.period_name)
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (uk.table_name, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (uk.table_name, u.attnum)
            WHERE a.attname NOT IN (p.start_column_name, p.end_column_name)
            ) AS a ON true
        WHERE uk.column_names <> a.column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT format('UPDATE periods.unique_keys SET unique_constraint = %L WHERE key_name = %L',
            c.conname, uk.key_name)
        FROM periods.unique_keys AS uk
        JOIN periods.periods AS p ON (p.table_name, p.period_name) = (uk.table_name, uk.period_name)
        CROSS JOIN LATERAL unnest(uk.column_names || ARRAY[p.start_column_name, p.end_column_name]) WITH ORDINALITY AS u (column_name, ordinality)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = uk.table_name
        WHERE NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (uk.table_name, uk.unique_constraint))
        GROUP BY uk.key_name, c.oid, c.conname
        HAVING format('UNIQUE (%s)', string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)) = pg_catalog.pg_get_constraintdef(c.oid)
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT format('UPDATE periods.unique_keys SET exclude_constraint = %L WHERE key_name = %L',
            c.conname, uk.key_name)
        FROM periods.unique_keys AS uk
        JOIN periods.periods AS p ON (p.table_name, p.period_name) = (uk.table_name, uk.period_name)
        CROSS JOIN LATERAL unnest(uk.column_names) WITH ORDINALITY AS u (column_name, ordinality)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = uk.table_name
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (uk.table_name, uk.exclude_constraint))
        GROUP BY uk.key_name, c.oid, c.conname, p.range_type, p.start_column_name, p.end_column_name
        HAVING format('EXCLUDE USING gist (%s, %I(%I, %I, ''[)''::text) WITH &&)',
                      string_agg(quote_ident(u.column_name) || ' WITH =', ', ' ORDER BY u.ordinality),
                      p.range_type,
                      p.start_column_name,
                      p.end_column_name) = pg_catalog.pg_get_constraintdef(c.oid)
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
    FOR r IN
        SELECT fk.key_name, fk.table_name, u.column_name
        FROM periods.foreign_keys AS fk
        CROSS JOIN LATERAL unnest(fk.column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (fk.table_name, u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is used in period foreign key "%"',
            r.column_name, r.table_name, r.key_name;
    END LOOP;

    /*
     * Since there can be multiple foreign keys, there is no reliable way to
     * know which trigger might belong to what, so just error out.
     */
    FOR r IN
        SELECT fk.key_name, fk.table_name, fk.fk_insert_trigger AS trigger_name
        FROM periods.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_name, fk.fk_insert_trigger))
        UNION ALL
        SELECT fk.key_name, fk.table_name, fk.fk_update_trigger AS trigger_name
        FROM periods.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_name, fk.fk_update_trigger))
        UNION ALL
        SELECT fk.key_name, uk.table_name, fk.uk_update_trigger AS trigger_name
        FROM periods.foreign_keys AS fk
        JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_name, fk.uk_update_trigger))
        UNION ALL
        SELECT fk.key_name, uk.table_name, fk.uk_delete_trigger AS trigger_name
        FROM periods.foreign_keys AS fk
        JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_name, fk.uk_delete_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in period foreign key "%"',
            r.trigger_name, r.table_name, r.key_name;
    END LOOP;

    ---
    --- system_versioning
    ---

    /* Nothing to do here */
END;
$function$;

