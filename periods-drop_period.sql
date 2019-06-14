CREATE FUNCTION periods.drop_period(table_name regclass, period_name name, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    period_row periods.periods;
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
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

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
     * Has the table been dropped already?  This could happen if the period is
     * being dropped by the health_check event trigger.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_name);

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

        IF NOT is_dropped AND purge THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_name, period_row.infinity_check_constraint);
            EXECUTE format('DROP TRIGGER %I ON %s', period_row.generated_always_trigger, table_name);
            EXECUTE format('DROP TRIGGER %I ON %s', period_row.write_history_trigger, table_name);
        END IF;
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
