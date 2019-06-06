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
        ORDER BY dobj.original DESC
    LOOP
        -- This doesn't work at all :(
        -- PERFORM periods.drop_period(table_name, period_name, 'CASCADE', true);

        RAISE EXCEPTION 'please drop periods on % first', table_name;
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
END;
$function$;

CREATE EVENT TRIGGER periods_health_check ON sql_drop EXECUTE FUNCTION periods.health_check();

/*

Don't allow new unique indexes to use a system_time period column. 11.7 SR 5b

*/
