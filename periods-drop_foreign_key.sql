CREATE FUNCTION periods.drop_foreign_key(table_name regclass, key_name name)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row periods.foreign_keys;
BEGIN
    IF table_name IS NULL AND key_name IS NULL THEN
        RAISE EXCEPTION 'no table or key name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM pg_advisory_xact_lock('periods.periods'::regclass::oid::integer, table_name::oid::integer);

    FOR foreign_key_row IN
        SELECT fk.*
        FROM periods.foreign_keys AS fk
        WHERE (fk.table_name = table_name OR table_name IS NULL)
          AND (fk.key_name = key_name OR key_name IS NULL)
    LOOP
        DELETE FROM periods.foreign_keys AS fk
        WHERE fk.key_name = foreign_key_row.key_name;

        EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_insert_trigger, foreign_key_row.table_name);
        EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_update_trigger, foreign_key_row.table_name);
        EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_update_trigger, foreign_key_row.table_name);
        EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_delete_trigger, foreign_key_row.table_name);
    END LOOP;

    RETURN true;
END;
$function$;

