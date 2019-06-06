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
        view_name := r.table_name || '__for_portion_of_' || r.period_name;
        trigger_name := 'for_portion_of_' || r.period_name;
        EXECUTE format('CREATE VIEW %1$I.%2$I AS TABLE %1$I.%3$I', r.schema_name, view_name, r.table_name);
        EXECUTE format('CREATE TRIGGER %I INSTEAD OF UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION periods.update_portion_of(%s, %I)'
            trigger_name, r.schema_name, r.table_name, r.period_name);
        INSERT INTO periods.for_portion_views (table_name, period_name, view_name, trigger_name)
            VALUES (format('%I.%I', r.schema_name, r.table_name), r.period_name, format('%I.%I', r.schema_name, view_name));
    END LOOP;

    RETURN true;
END;
$function$;
