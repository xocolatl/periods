CREATE FUNCTION periods.generate_name(table_name name, column_names name[], period_name name)
 RETURNS name
 IMMUTABLE STRICT
 LANGUAGE plpgsql
AS
$function$
DECLARE
    result text;
BEGIN
    /*
     * We want a name of the form "table_column1_column2_period_NN".  We always
     * want the table and the period so we'll add the columns one by one until
     * we run out of room.
     *
     * If "table_period_NN" is already too long, we want to keep as much of
     * each name as possible so we trim the longest one until they both fit.
     */
    WHILE octet_length(format('%s_%s_99', table_name, period_name)) > 63 LOOP
        IF octet_length(table_name) > octet_length(period_name) THEN
            table_name := left(table_name, -1);
        ELSE
            period_name := left(period_name, -1);
        END IF;
    END LOOP;

    result := table_name;
    WHILE octet_length(format('%s_%s_%s_99', table_name, column_names[1], period_name)) < 63 AND column_names <> '{}' LOOP
        result := result || '_' || column_names[1];
        column_names := column_names[2:];
    END LOOP;

    /* The _NN part will be added by the caller if needed */
    RETURN result || '_' || period_name;
END;
$function$;

