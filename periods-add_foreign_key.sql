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
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

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
    key_name := periods.generate_name(
        (SELECT c.relname FROM pg_class AS c WHERE c.oid = table_name),
        column_names, period_name);
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
    fk_insert_name := key_name || '_fk_insert';
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER INSERT ON %s FROM %s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION periods.fk_insert_check(%L)',
        fk_insert_name, table_name, unique_row.table_name, key_name);
    fk_update_name := key_name || '_fk_update';
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER UPDATE ON %s FROM %s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION periods.fk_update_check(%L)',
        fk_update_name, table_name, unique_row.table_name, key_name);
    uk_update_name := key_name || '_uk_update';
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER UPDATE ON %s FROM %s%s FOR EACH ROW EXECUTE FUNCTION periods.uk_update_check(%L)',
        uk_update_name, unique_row.table_name, table_name, upd_action, key_name);
    uk_delete_name = key_name || '_uk_delete';
    EXECUTE format('CREATE CONSTRAINT TRIGGER %I AFTER DELETE ON %s FROM %s%s FOR EACH ROW EXECUTE FUNCTION periods.uk_delete_check(%L)',
        uk_delete_name, unique_row.table_name, table_name, del_action, key_name);

    INSERT INTO periods.foreign_keys (key_name, table_name, column_names, period_name, unique_key, match_type, update_action, delete_action,
                                      fk_insert_trigger, fk_update_trigger, uk_update_trigger, uk_delete_trigger)
    VALUES (key_name, table_name, column_names, period_name, unique_row.key_name, match_type, update_action, delete_action,
            fk_insert_name, fk_update_name, uk_update_name, uk_delete_name);

    /* Validate the constraint on existing data */
    PERFORM periods.validate_foreign_key(key_name);

    RETURN key_name;
END;
$function$;

