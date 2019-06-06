CREATE FUNCTION periods.remove_period(table_name regclass, period_name name, cascade boolean DEFAULT false, if_exists boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
BEGIN
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

    IF cascade THEN
        -- PERFORM remove_foreign_key(fk.name, true)
        -- FROM periods.foreign_keys AS fk
        -- WHERE (fk.table_name, fk.period_name) = (table_name, period_name);

        -- PERFORM remove_unique_key(uk.name, true);
        -- FROM periods.unique_keys AS uk
        -- WHERE (uk.table_name, uk.period_name) = (table_name, period_name);
    ELSIF EXISTS (
        SELECT FROM periods.unique_keys AS uk WHERE (uk.table_name, uk.period_name) = (table_name, period_name)     
        UNION ALL
        SELECT FROM periods.foreign_keys AS fk WHERE (fk.table_name, fk.period_name) = (table_name, period_name)     
        )
    THEN
        RAISE EXCEPTION 'cannot drop period % of table % because other objects depend on it', period_name, table_name;
    END IF; 

    DELETE FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_name, period_name);

    IF NOT FOUND AND NOT if_exists THEN
        RAISE EXCEPTION 'table % has no period %', table_name, period_name;
    END IF;

    RETURN true;
END;
$function$;

