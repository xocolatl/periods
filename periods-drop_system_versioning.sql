CREATE FUNCTION periods.drop_system_versioning(table_name regclass, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    system_versioning_row periods.system_versioning;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

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

    /* Drop the functions. */
    EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of::regprocedure, drop_behavior);
    EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between::regprocedure, drop_behavior);
    EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric::regprocedure, drop_behavior);
    EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to::regprocedure, drop_behavior);

    /* Drop the "with_history" view. */
    EXECUTE format('DROP VIEW %s %s', system_versioning_row.view_name, drop_behavior);

    /*
     * SQL:2016 11.30 GR 2 says "Every row of T that corresponds to a
     * historical system row is effectively deleted at the end of the SQL-
     * statement." but we leave the history table intact in case the user
     * merely wants to make some DDL changes and hook things back up again.
     *
     * The purge parameter tells us that the user really wants to get rid of it
     * all.
     */
    IF purge THEN
        PERFORM periods.drop_period(system_versioning_row.history_table_name, drop_behavior, purge);
        EXECUTE format('DROP TABLE %s %s', system_versioning_row.history_table_name, drop_behavior);
    END IF;

    RETURN true;
END;
$function$;

