CREATE FUNCTION periods.generated_always_as_row_start_end()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    jnew jsonb;
	start_column_name name;
    end_column_name name;
BEGIN
    SELECT p.start_column_name, p.end_column_name
    INTO start_column_name, end_column_name
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (TG_RELID, 'system_time');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'period for SYSTEM_TIME on table "%" not found', TG_TABLE_NAME;
    END IF;

    /* Set the new timestamps */
    jnew := row_to_json(NEW);
    jnew := jsonb_set(jnew, ARRAY[start_column_name], to_jsonb(transaction_timestamp()));
    jnew := jsonb_set(jnew, ARRAY[end_column_name], to_jsonb('infinity'::timestamp with time zone));
    RETURN jsonb_populate_record(NEW, jnew);
END;
$function$;

