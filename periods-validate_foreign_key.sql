/*
 * This function either returns true or raises an exception.
 */
CREATE FUNCTION periods.validate_foreign_key(foreign_key_name name, row_data jsonb DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_info record;
    uksql text;
    fksql text;
    ukrow record;
    fkrow record;
    agg record;
    row_clause text DEFAULT '';

    FKSQL_TEMPLATE CONSTANT text :=
        'SELECT DISTINCT '
        '    TREAT(row_to_json(ROW(%3$s)) AS jsonb) AS key_values, '
        '    %4$I AS start_value, '
        '    %5$I AS end_value '
        'FROM %1$I.%2$I '
        'WHERE NOT ((%3$s) IS NULL)'
        '%6$s';

    UKSQL_TEMPLATE CONSTANT text :=
        'SELECT '
        '    %I AS start_value, '
        '    %I AS end_value '
        'FROM %I.%I '
        'WHERE TREAT(row_to_json(ROW(%s)) AS jsonb) = $1 '
        '  AND %I < $3 '
        '  AND %I >= $2 '
        'ORDER BY %I '
        'FOR KEY SHARE';
BEGIN
    SELECT fn.nspname AS fk_schema_name,
           fc.relname AS fk_table_name,
           fk.column_names AS fk_column_names,
           fp.period_name AS fk_period_name,
           fp.start_column_name AS fk_start_column_name,
           fp.end_column_name AS fk_end_column_name,

           un.nspname AS uk_schema_name,
           uc.relname AS uk_table_name,
           uk.column_names AS uk_column_names,
           up.period_name AS uk_period_name,
           up.start_column_name AS uk_start_column_name,
           up.end_column_name AS uk_end_column_name,

           fk.match_type,
           fk.update_action,
           fk.delete_action
    INTO foreign_key_info
    FROM periods.foreign_keys AS fk
    JOIN periods.periods AS fp ON (fp.table_name, fp.period_name) = (fk.table_name, fk.period_name)
    JOIN pg_class AS fc ON fc.oid = fk.table_name
    JOIN pg_namespace AS fn ON fn.oid = fc.relnamespace
    JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
    JOIN periods.periods AS up ON (up.table_name, up.period_name) = (uk.table_name, uk.period_name)
    JOIN pg_class AS uc ON uc.oid = uk.table_name
    JOIN pg_namespace AS un ON un.oid = uc.relnamespace
    WHERE fk.key_name = foreign_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'foreign key "%" not found', foreign_key_name;
    END IF;

    /*
     * Now that we have all of our names, we can see if there are any nulls in
     * the row we were given (if we were given one).
     */
    IF row_data IS NOT NULL THEN
        DECLARE
            column_name name;
            has_nulls boolean;
            all_nulls boolean;
            cols text[] DEFAULT '{}';
            vals text[] DEFAULT '{}';
        BEGIN
            FOREACH column_name IN ARRAY foreign_key_info.fk_column_names LOOP
                has_nulls := has_nulls OR row_data->>column_name IS NULL;
                all_nulls := all_nulls IS NOT false AND row_data->>column_name IS NULL;
                cols := cols || quote_ident(column_name);
                vals := vals || quote_literal(row_data->>column_name);
            END LOOP;

            IF all_nulls THEN
                /*
                 * If there are no values at all, all three types pass.
                 *
                 * Period columns are by definition NOT NULL so the FULL MATCH
                 * type is only concerned with the non-period columns of the
                 * constraint.  SQL:2016 4.23.3.3
                 */
                RETURN true;
            END IF;

            IF has_nulls THEN
                CASE foreign_key_info.match_type
                    WHEN 'SIMPLE' THEN
                        RETURN true;
                    WHEN 'PARTIAL' THEN
                        RAISE EXCEPTION 'partial not implemented';
                    WHEN 'FULL' THEN
                        RAISE EXCEPTION 'foreign key violated (nulls in FULL)';
                END CASE;
            END IF;

            row_clause := format(' AND (%s) = (%s)', array_to_string(cols, ', '), array_to_string(vals, ', '));
        END;
    END IF;

    fksql := format(FKSQL_TEMPLATE,
                    foreign_key_info.fk_schema_name,
                    foreign_key_info.fk_table_name,
                    array_to_string(foreign_key_info.fk_column_names, ', '),
                    foreign_key_info.fk_start_column_name,
                    foreign_key_info.fk_end_column_name,
                    row_clause);

    FOR fkrow IN EXECUTE fksql LOOP
        IF jsonb_strip_nulls(fkrow.key_values) <> fkrow.key_values THEN -- the row has at least one null
            CASE foreign_key_info.match_type
                WHEN 'SIMPLE' THEN
                    CONTINUE;
                WHEN 'PARTIAL' THEN
                    RAISE EXCEPTION 'partial not implemented';
                WHEN 'FULL' THEN
                    RAISE EXCEPTION 'foreign key violated (nulls in FULL)';
            END CASE;
        END IF;

        uksql := format(UKSQL_TEMPLATE,
                        foreign_key_info.uk_start_column_name,
                        foreign_key_info.uk_end_column_name,
                        foreign_key_info.uk_schema_name,
                        foreign_key_info.uk_table_name,
                        array_to_string(foreign_key_info.uk_column_names, ', '),
                        foreign_key_info.uk_start_column_name,
                        foreign_key_info.uk_end_column_name,
                        foreign_key_info.uk_start_column_name);

        agg := NULL;
        FOR ukrow IN
            EXECUTE uksql
            USING fkrow.key_values, fkrow.start_value, fkrow.end_value
        LOOP
            IF agg IS NULL THEN
                SELECT ukrow.start_value, ukrow.end_value INTO agg;
                EXIT WHEN agg.start_value > fkrow.start_value;
            ELSE
                IF ukrow.start_value = agg.end_value THEN
                    agg.end_value := ukrow.end_value;

                    /*
                     * If this condition is true, we *should* be on the last
                     * iteration anyway because the periods can't overlap
                     */
                    EXIT WHEN agg.end_value >= fkrow.end_value;
                ELSE
                    /* The periods aren't consecutive, exit the loop to report the error */
                    EXIT;
                END IF;
            END IF;
        END LOOP;

        IF agg IS NULL THEN
            RAISE EXCEPTION 'foreign key violated (no rows match)';
        END IF;

        IF agg.start_value > fkrow.start_value OR agg.end_value < fkrow.end_value THEN
            RAISE EXCEPTION 'foreign key violated (period not complete)';
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;

