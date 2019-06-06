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
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

    SELECT p.*
    INTO period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_name, period_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'period "%" does not exist', period_name;
    END IF;

    /* For convenience, put the period's attnums in an array */
    period_attnums := ARRAY[
        (SELECT a.attnum FROM pg_attribute AS a WHERE (a.attrelid, a.attname) = (period_row.table_name, period_row.start_column_name)),
        (SELECT a.attnum FROM pg_attribute AS a WHERE (a.attrelid, a.attname) = (period_row.table_name, period_row.end_column_name))
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
        FROM pg_constraint AS c
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
        FROM pg_constraint AS c
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
    key_name := periods.generate_name((SELECT c.relname FROM pg_class AS c WHERE c.oid = table_name),
                                  column_names, period_name);
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
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_name;

        EXECUTE sql;
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL THEN
        SELECT c.conname, c.conindid
        INTO unique_constraint, unique_index
        FROM pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_name, 'u')
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    /* If we don't already have an exclude_constraint, it must be the one with the highest oid */
    IF exclude_constraint IS NULL THEN
        SELECT c.conname, c.conindid
        INTO exclude_constraint, exclude_index
        FROM pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_name, 'x')
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    INSERT INTO periods.unique_keys (key_name, table_name, column_names, period_name, unique_constraint, exclude_constraint)
    VALUES (key_name, table_name, column_names, period_name, unique_constraint, exclude_constraint);

    RETURN true;
END;
$function$;

