CREATE FUNCTION periods.drop_unique_key(table_name regclass, key_name name, drop_behavior periods.drop_behavior DEFAULT 'RESTRICT', purge boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row periods.foreign_keys;
    unique_key_row periods.unique_keys;
BEGIN
    IF table_name IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

    FOR unique_key_row IN
        SELECT uk.*
        FROM periods.unique_keys AS uk
        WHERE uk.table_name = table_name
          AND (uk.key_name = key_name OR key_name IS NULL)
    LOOP
        /* Cascade to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.key_name
            FROM periods.foreign_keys AS fk
            WHERE fk.unique_key = unique_key_row.key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.key_name, foreign_key_row.key_name, foreign_key_row.table_name;
            END IF;

            PERFORM drop_foreign_key(NULL, foreign_key_row.key_name);
        END LOOP;

        DELETE FROM periods.unique_keys AS uk
        WHERE uk.key_name = unique_key_row.key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF purge AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            WHERE c.oid = unique_key_row.table_name)
        THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, DROP CONSTRAINT %I',
                unique_key_row.table_name, unique_key_row.unique_constraint, unique_key_row.exclude_constraint);
        END IF;
    END LOOP;
END;
$function$;
