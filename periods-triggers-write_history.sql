CREATE FUNCTION periods.write_history()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
#variable_conflict use_variable
DECLARE
    jnew jsonb;
    jold jsonb;
    table_name regclass;
	start_column_name name;
    end_column_name name;
    history_schema_name name;
    history_table_name name;
BEGIN
    /*
     * If this is not a table with system versioning, just do nothing.  The
     * trigger exists for all SYSTEM_TIME periods.
     */
    IF NOT EXISTS (SELECT FROM periods.system_versioning AS sv WHERE sv.table_name = TG_RELID) THEN
        RETURN NULL;
    END IF;

    SELECT p.table_name, p.start_column_name, p.end_column_name
    INTO table_name, start_column_name, end_column_name
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (TG_RELID, 'system_time');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'period for SYSTEM_TIME on table "%" not found', TG_TABLE_NAME;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        jnew := row_to_json(NEW);
        IF (jnew->>start_column_name)::timestamp with time zone <> transaction_timestamp() THEN
            RAISE EXCEPTION 'system versioned start column has been tampered with';
        END IF;

		IF (jnew->>end_column_name)::timestamp with time zone <> 'infinity'::timestamp with time zone THEN
            RAISE EXCEPTION 'system versioned end column has been tampered with';
        END IF;
    END IF;

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        jold := row_to_json(OLD);
		IF (jold->>end_column_name)::timestamp with time zone = transaction_timestamp() THEN
            /* This row has already been in updated this transaction; do nothing. */
			RETURN NULL;
		END IF;

        jold := jsonb_set(jold, ARRAY[end_column_name], to_jsonb(transaction_timestamp()));

        SELECT n.nspname, c.relname
        INTO history_schema_name, history_table_name
        FROM periods.system_versioning AS r
        JOIN pg_class AS c ON c.oid = r.history_table_name
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE r.table_name = table_name;
         
        IF NOT FOUND THEN
            RAISE EXCEPTION 'history table not found';
        END IF;

        EXECUTE format('INSERT INTO %I.%I VALUES (($1).*)', history_schema_name, history_table_name)
        USING jsonb_populate_record(OLD, jold);
    END IF;

    RETURN NULL;
END;
$function$;

