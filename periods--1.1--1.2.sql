/* Fix up access controls */

GRANT USAGE ON SCHEMA periods TO PUBLIC;
REVOKE ALL
    ON TABLE periods.periods, periods.system_time_periods,
             periods.for_portion_views, periods.unique_keys,
             periods.foreign_keys, periods.system_versioning
    FROM PUBLIC;
GRANT SELECT
    ON TABLE periods.periods, periods.system_time_periods,
             periods.for_portion_views, periods.unique_keys,
             periods.foreign_keys, periods.system_versioning
    TO PUBLIC;

ALTER FUNCTION periods.add_for_portion_view(regclass,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_foreign_key(regclass,name[],name,name,periods.fk_match_types,periods.fk_actions,periods.fk_actions,name,name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_period(regclass,name,name,name,regtype,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_system_time_period(regclass,name,name,name,name,name,name,name,name[]) SECURITY DEFINER;
ALTER FUNCTION periods.add_system_versioning(regclass,name,name,name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_unique_key(regclass,name[],name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.drop_for_portion_view(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_foreign_key(regclass,name) SECURITY DEFINER;
ALTER FUNCTION periods.drop_period(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_system_versioning(regclass,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_unique_key(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.generated_always_as_row_start_end() SECURITY DEFINER;
ALTER FUNCTION periods.health_checks() SECURITY DEFINER;
ALTER FUNCTION periods.rename_following() SECURITY DEFINER;
ALTER FUNCTION periods.set_system_time_period_excluded_columns(regclass,name[]) SECURITY DEFINER;
ALTER FUNCTION periods.truncate_system_versioning() SECURITY DEFINER;
ALTER FUNCTION periods.write_history() SECURITY DEFINER;

CREATE OR REPLACE FUNCTION periods.add_system_versioning(
    table_class regclass,
    history_table_name name DEFAULT NULL,
    view_name name DEFAULT NULL,
    function_as_of_name name DEFAULT NULL,
    function_between_name name DEFAULT NULL,
    function_between_symmetric_name name DEFAULT NULL,
    function_from_to_name name DEFAULT NULL)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    schema_name name;
    table_name name;
    table_owner regrole;
    persistence "char";
    kind "char";
    period_row periods.periods;
    history_table_id oid;
BEGIN
    IF table_class IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_class);

    /*
     * REFERENCES:
     *     SQL:2016 4.15.2.2
     *     SQL:2016 11.3 SR 2.3
     *     SQL:2016 11.3 GR 1.c
     *     SQL:2016 11.29
     */

    /* Already registered? SQL:2016 11.29 SR 5 */
    IF EXISTS (SELECT FROM periods.system_versioning AS r WHERE r.table_name = table_class) THEN
        RAISE EXCEPTION 'table already has SYSTEM VERSIONING';
    END IF;

    /* Must be a regular persistent base table. SQL:2016 11.29 SR 2 */

    SELECT n.nspname, c.relname, c.relowner, c.relpersistence, c.relkind
    INTO schema_name, table_name, table_owner, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_class;

    IF kind <> 'r' THEN
        /*
         * The main reason partitioned tables aren't supported yet is simply
         * because I haven't put any thought into it.
         * Maybe it's trivial, maybe not.
         */
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;

        RAISE EXCEPTION 'relation % is not a table', $1;
    END IF;

    IF persistence <> 'p' THEN
        /*
         * We could probably accept unlogged tables if the history table is
         * also unlogged, but what's the point?
         */
        RAISE EXCEPTION 'table "%" must be persistent', table_class;
    END IF;

    /* We need a SYSTEM_TIME period. SQL:2016 11.29 SR 4 */
    SELECT p.*
    INTO period_row
    FROM periods.periods AS p
    WHERE (p.table_name, p.period_name) = (table_class, 'system_time');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'no period for SYSTEM_TIME found for table %', table_class;
    END IF;

    /* Get all of our "fake" infrastructure ready */
    history_table_name := coalesce(history_table_name, periods._choose_name(ARRAY[table_name], 'history'));
    view_name := coalesce(view_name, periods._choose_name(ARRAY[table_name], 'with_history'));
    function_as_of_name := coalesce(function_as_of_name, periods._choose_name(ARRAY[table_name], '_as_of'));
    function_between_name := coalesce(function_between_name, periods._choose_name(ARRAY[table_name], '_between'));
    function_between_symmetric_name := coalesce(function_between_symmetric_name, periods._choose_name(ARRAY[table_name], '_between_symmetric'));
    function_from_to_name := coalesce(function_from_to_name, periods._choose_name(ARRAY[table_name], '_from_to'));

    /*
     * Create the history table.  If it already exists we check that all the
     * columns match but otherwise we trust the user.  Perhaps the history
     * table was disconnected in order to change the schema (a case which is
     * not defined by the SQL standard).  Or perhaps the user wanted to
     * partition the history table.
     *
     * There shouldn't be any concurrency issues here because our main catalog
     * is locked.
     */
    SELECT c.oid
    INTO history_table_id
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE (n.nspname, c.relname) = (schema_name, history_table_name);

    IF FOUND THEN
        /* Don't allow any periods on the history table (this might be relaxed later) */
        IF EXISTS (SELECT FROM periods.periods AS p WHERE p.table_name = history_table_id) THEN
            RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have periods';
        END IF;

        /*
         * The query to the attributes is harder than one would think because
         * we need to account for dropped columns.  Basically what we're
         * looking for is that all columns have the same name, type, and
         * collation.
         */
        IF EXISTS (
            WITH
            L (attname, atttypid, atttypmod, attcollation) AS (
                SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = table_class
                  AND NOT a.attisdropped
            ),
            R (attname, atttypid, atttypmod, attcollation) AS (
                SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = history_table_id
                  AND NOT a.attisdropped
            )
            SELECT FROM L NATURAL FULL JOIN R
            WHERE L.attname IS NULL OR R.attname IS NULL)
        THEN
            RAISE EXCEPTION 'base table "%" and history table "%" are not compatible',
                table_class, history_table_id::regclass;
        END IF;

        /* Make sure the owner is correct */
        EXECUTE format('ALTER TABLE %s OWNER TO %I', history_table_id::regclass, table_owner);
    ELSE
        EXECUTE format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', schema_name, history_table_name, table_name);
        history_table_id := format('%I.%I', schema_name, history_table_name)::regclass;

        EXECUTE format('ALTER TABLE %1$I.%2$I OWNER TO %3$I', schema_name, history_table_name, table_owner);

        RAISE NOTICE 'history table "%" created for "%", be sure to index it properly',
            history_table_id::regclass, table_class;
    END IF;

    /* Create the "with history" view.  This one we do want to error out on if it exists. */
    EXECUTE format(
        /*
         * The query we really here want is
         *
         *     CREATE VIEW view_name AS
         *         TABLE table_name
         *         UNION ALL CORRESPONDING
         *         TABLE history_table_name
         *
         * but PostgreSQL doesn't support that syntax (yet), so we have to do
         * it manually.
         */
        'CREATE VIEW %1$I.%2$I AS SELECT %5$s FROM %1$I.%3$I UNION ALL SELECT %5$s FROM %1$I.%4$I',
        schema_name, view_name, table_name, history_table_name,
        (SELECT string_agg(a.attname, ', ' ORDER BY a.attnum)
         FROM pg_attribute AS a
         WHERE a.attrelid = table_class
           AND a.attnum > 0
           AND NOT a.attisdropped
        ));
    EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %3$I', schema_name, view_name, table_owner);

    /*
     * Create functions to simulate the system versioned grammar.  These must
     * be inlinable for any kind of performance.
     */
    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE %4$I <= $1 AND %5$I > $1'
        $$, schema_name, function_as_of_name, view_name, period_row.start_column_name, period_row.end_column_name);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone) OWNER TO %3$I',
        schema_name, function_as_of_name, table_owner);

    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE $1 <= $2 AND %5$I > $1 AND %4$I <= $2'
        $$, schema_name, function_between_name, view_name, period_row.start_column_name, period_row.end_column_name);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I',
        schema_name, function_between_name, table_owner);

    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE %5$I > least($1, $2) AND %4$I <= greatest($1, $2)'
        $$, schema_name, function_between_symmetric_name, view_name, period_row.start_column_name, period_row.end_column_name);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I',
        schema_name, function_between_symmetric_name, table_owner);

    EXECUTE format(
        $$
        CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
         RETURNS SETOF %1$I.%3$I
         LANGUAGE sql
         STABLE
        AS 'SELECT * FROM %1$I.%3$I WHERE $1 < $2 AND %5$I > $1 AND %4$I < $2'
        $$, schema_name, function_from_to_name, view_name, period_row.start_column_name, period_row.end_column_name);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I',
        schema_name, function_from_to_name, table_owner);

    /* Register it */
    INSERT INTO periods.system_versioning (table_name, period_name, history_table_name, view_name,
                                           func_as_of, func_between, func_between_symmetric, func_from_to)
    VALUES (
        table_class,
        'system_time',
        format('%I.%I', schema_name, history_table_name),
        format('%I.%I', schema_name, view_name),
        format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name)::regprocedure,
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name)::regprocedure,
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name)::regprocedure,
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)::regprocedure
    );
END;
$function$;

CREATE OR REPLACE FUNCTION periods.health_checks()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    cmd text;
    r record;
BEGIN
    /* Make sure that all of our tables are still persistent */
    FOR r IN
        SELECT p.table_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_class AS c ON c.oid = p.table_name
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'table "%" must remain persistent because it has periods',
            r.table_name;
    END LOOP;

    /* And the history tables, too */
    FOR r IN
        SELECT sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_class AS c ON c.oid = sv.history_table_name
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'history table "%" must remain persistent because it has periods',
            r.table_name;
    END LOOP;

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        SELECT format('ALTER %s %s OWNER TO %I',
            CASE ht.relkind
                WHEN 'r' THEN 'TABLE'
                WHEN 'v' THEN 'VIEW'
            END,
            ht.oid::regclass, t.relowner::regrole)
        FROM periods.system_versioning AS sv
        JOIN pg_class AS t ON t.oid = sv.table_name
        JOIN pg_class AS ht ON ht.oid IN (sv.history_table_name, sv.view_name)
        WHERE t.relowner <> ht.relowner

        UNION ALL

        SELECT format('ALTER VIEW %s OWNER TO %I', fpt.oid::regclass, t.relowner::regrole)
        FROM periods.for_portion_views AS fpv
        JOIN pg_class AS t ON t.oid = fpv.table_name
        JOIN pg_class AS fpt ON fpt.oid = fpv.view_name
        WHERE t.relowner <> fpt.relowner

        UNION ALL

        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        FROM periods.system_versioning AS sv
        JOIN pg_class AS t ON t.oid = sv.table_name
        JOIN pg_proc AS p ON p.oid IN (sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to)
        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;
END;
$function$;
