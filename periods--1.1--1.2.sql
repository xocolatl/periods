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

ALTER TABLE periods.system_versioning
    ALTER COLUMN func_as_of SET DATA TYPE text,
    ALTER COLUMN func_between SET DATA TYPE text,
    ALTER COLUMN func_between_symmetric SET DATA TYPE text,
    ALTER COLUMN func_from_to SET DATA TYPE text
;

ALTER FUNCTION periods.add_for_portion_view(regclass,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_foreign_key(regclass,name[],name,name,periods.fk_match_types,periods.fk_actions,periods.fk_actions,name,name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_period(regclass,name,name,name,regtype,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_system_time_period(regclass,name,name,name,name,name,name,name,name[]) SECURITY DEFINER;
ALTER FUNCTION periods.add_system_versioning(regclass,name,name,name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.add_unique_key(regclass,name[],name,name,name,name) SECURITY DEFINER;
ALTER FUNCTION periods.drop_for_portion_view(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_foreign_key(regclass,name) SECURITY DEFINER;
ALTER FUNCTION periods.drop_period(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_protection() SECURITY DEFINER;
ALTER FUNCTION periods.drop_system_versioning(regclass,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_system_time_period(table_name regclass,drop_behavior periods.drop_behavior,purge boolean) SECURITY DEFINER;
ALTER FUNCTION periods.drop_unique_key(regclass,name,periods.drop_behavior,boolean) SECURITY DEFINER;
ALTER FUNCTION periods.generated_always_as_row_start_end() SECURITY DEFINER;
ALTER FUNCTION periods.health_checks() SECURITY DEFINER;
ALTER FUNCTION periods.rename_following() SECURITY DEFINER;
ALTER FUNCTION periods.set_system_time_period_excluded_columns(regclass,name[]) SECURITY DEFINER;
ALTER FUNCTION periods.truncate_system_versioning() SECURITY DEFINER;
ALTER FUNCTION periods.write_history() SECURITY DEFINER;

CREATE OR REPLACE FUNCTION periods.add_for_portion_view(table_name regclass DEFAULT NULL, period_name name DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    view_name name;
    trigger_name name;
BEGIN
    /*
     * If table_name and period_name are specified, then just add the views for that.
     *
     * If no period is specified, add the views for all periods of the table.
     *
     * If no table is specified, add the views everywhere.
     *
     * If no table is specified but a period is, that doesn't make any sense.
     */
    IF table_name IS NULL AND period_name IS NOT NULL THEN
        RAISE EXCEPTION 'cannot specify period name without table name';
    END IF;

    /* Can't use FOR PORTION OF on SYSTEM_TIME columns */
    IF period_name = 'system_time' THEN
        RAISE EXCEPTION 'cannot use FOR PORTION OF on SYSTEM_TIME periods';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM periods._serialize(table_name);

    /*
     * We require the table to have a primary key, so check to see if there is
     * one.  This requires a lock on the table so no one removes it after we
     * check and before we commit.
     */
    EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE', table_name);

    /* Now check for the primary key */
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_name, 'p'))
    THEN
        RAISE EXCEPTION 'table "%" must have a primary key', table_name;
    END IF;

    FOR r IN
        SELECT n.nspname AS schema_name, c.relname AS table_name, c.relowner AS table_owner, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_class AS c ON c.oid = p.table_name
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE (table_name IS NULL OR p.table_name = table_name)
          AND (period_name IS NULL OR p.period_name = period_name)
          AND p.period_name <> 'system_time'
          AND NOT EXISTS (
                SELECT FROM periods.for_portion_views AS _fpv
                WHERE (_fpv.table_name, _fpv.period_name) = (p.table_name, p.period_name))
    LOOP
        view_name := periods._choose_portion_view_name(r.table_name, r.period_name);
        trigger_name := 'for_portion_of_' || r.period_name;
        EXECUTE format('CREATE VIEW %1$I.%2$I AS TABLE %1$I.%3$I', r.schema_name, view_name, r.table_name);
        EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %s', r.schema_name, view_name, r.table_owner::regrole);
        EXECUTE format('CREATE TRIGGER %I INSTEAD OF UPDATE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE periods.update_portion_of()',
            trigger_name, r.schema_name, view_name);
        INSERT INTO periods.for_portion_views (table_name, period_name, view_name, trigger_name)
            VALUES (format('%I.%I', r.schema_name, r.table_name), r.period_name, format('%I.%I', r.schema_name, view_name), trigger_name);
    END LOOP;

    RETURN true;
END;
$function$;

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
    sql text;
    grantees text;
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

        /*
         * Remove all privileges other than SELECT from everyone on the history
         * table.  We do this without error because some privileges may have
         * been added in order to do maintenance while we were disconnected.
         *
         * We start by doing the table owner because that will make sure we
         * don't have NULL in pg_class.relacl.
         */
        --EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE %s FROM %I',
            --history_table_id::regclass, table_owner);
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
         * The query we really want here is
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
        (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
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

    /* Set privileges on history objects */
    FOR sql IN
        SELECT format('REVOKE ALL ON %s %s FROM %s',
                      CASE object_type
                          WHEN 'r' THEN 'TABLE'
                          WHEN 'v' THEN 'TABLE'
                          WHEN 'f' THEN 'FUNCTION'
                      ELSE 'ERROR'
                      END,
                      string_agg(DISTINCT object_name, ', '),
                      string_agg(DISTINCT quote_ident(COALESCE(a.rolname, 'public')), ', '))
        FROM (
            SELECT c.relkind AS object_type,
                   c.oid::regclass::text AS object_name,
                   acl.grantee AS grantee
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
            WHERE n.nspname = schema_name
              AND c.relname IN (history_table_name, view_name)

            UNION ALL

            SELECT 'f',
                   p.oid::regprocedure::text,
                   acl.grantee
            FROM pg_proc AS p
            CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
            WHERE p.oid = ANY (ARRAY[
                    format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name)::regprocedure,
                    format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name)::regprocedure,
                    format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name)::regprocedure,
                    format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)::regprocedure
                ])
        ) AS objects
        LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
        GROUP BY objects.object_type
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR grantees IN
        SELECT string_agg(acl.grantee::regrole::text, ', ')
        FROM pg_class AS c
        CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
        WHERE c.oid = table_class
          AND acl.privilege_type = 'SELECT'
    LOOP
        EXECUTE format('GRANT SELECT ON TABLE %1$I.%2$I, %1$I.%3$I TO %4$s',
                       schema_name, history_table_name, view_name, grantees);
        EXECUTE format('GRANT EXECUTE ON FUNCTION %s, %s, %s, %s TO %s',
                       format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name)::regprocedure,
                       format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name)::regprocedure,
                       format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name)::regprocedure,
                       format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)::regprocedure,
                       grantees);
    END LOOP;

    /* Register it */
    INSERT INTO periods.system_versioning (table_name, period_name, history_table_name, view_name,
                                           func_as_of, func_between, func_between_symmetric, func_from_to)
    VALUES (
        table_class,
        'system_time',
        format('%I.%I', schema_name, history_table_name),
        format('%I.%I', schema_name, view_name),
        format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)
    );
END;
$function$;

CREATE OR REPLACE FUNCTION periods.drop_protection()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_name regclass;
    period_name name;
BEGIN
    /*
     * This function is called after the fact, so we have to just look to see
     * if anything is missing in the catalogs if we just store the name and not
     * a reg* type.
     */

    ---
    --- periods
    ---

    /* If one of our tables is being dropped, remove references to it */
    FOR table_name, period_name IN
        SELECT p.table_name, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.table_name
        WHERE dobj.object_type = 'table'
        ORDER BY dobj.ordinality
    LOOP
        PERFORM periods.drop_period(table_name, period_name, 'CASCADE', true);
    END LOOP;

    /*
     * If a column belonging to one of our periods is dropped, we need to reject that.
     * SQL:2016 11.23 SR 6
     */
    FOR r IN
        SELECT dobj.object_identity, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_attribute AS sa ON (sa.attrelid, sa.attname) = (p.table_name, p.start_column_name)
        JOIN pg_catalog.pg_attribute AS ea ON (ea.attrelid, ea.attname) = (p.table_name, p.end_column_name)
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.table_name AND dobj.objsubid IN (sa.attnum, ea.attnum)
        WHERE dobj.object_type = 'table column'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop column "%" because it is part of the period "%"',
            r.object_identity, r.period_name;
    END LOOP;

    /* Also reject dropping the rangetype */
    FOR r IN
        SELECT dobj.object_identity, p.table_name, p.period_name
        FROM periods.periods AS p
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.range_type
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop rangetype "%" because it is used in period "%" on table "%"',
            r.object_identity, r.period_name, r.table_name;
    END LOOP;

    ---
    --- system_time_periods
    ---

    /* Complain if the infinity CHECK constraint is missing. */
    FOR r IN
        SELECT p.table_name, p.infinity_check_constraint
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (p.table_name, p.infinity_check_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in SYSTEM_TIME period',
            r.infinity_check_constraint, r.table_name;
    END LOOP;

    /* Complain if the GENERATED ALWAYS AS ROW START/END trigger is missing. */
    FOR r IN
        SELECT p.table_name, p.generated_always_trigger
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (p.table_name, p.generated_always_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
            r.generated_always_trigger, r.table_name;
    END LOOP;

    /* Complain if the write_history trigger is missing. */
    FOR r IN
        SELECT p.table_name, p.write_history_trigger
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (p.table_name, p.write_history_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
            r.write_history_trigger, r.table_name;
    END LOOP;

    /* Complain if the TRUNCATE trigger is missing. */
    FOR r IN
        SELECT p.table_name, p.truncate_trigger
        FROM periods.system_time_periods AS p
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (p.table_name, p.truncate_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
            r.truncate_trigger, r.table_name;
    END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
    FOR r IN
        SELECT stp.table_name, u.column_name
        FROM periods.system_time_periods AS stp
        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from SYSTEM VERSIONING',
            r.column_name, r.table_name;
    END LOOP;

    ---
    --- for_portion_views
    ---

    /* Reject dropping the FOR PORTION OF view. */
    FOR r IN
        SELECT dobj.object_identity
        FROM periods.for_portion_views AS fpv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = fpv.view_name
        WHERE dobj.object_type = 'view'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop view "%", call "periods.drop_for_portion_view()" instead',
            r.object_identity;
    END LOOP;

    /* Complain if the FOR PORTION OF trigger is missing. */
    FOR r IN
        SELECT fpv.table_name, fpv.period_name, fpv.view_name, fpv.trigger_name
        FROM periods.for_portion_views AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fpv.view_name, fpv.trigger_name))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on view "%" because it is used in FOR PORTION OF view for period "%" on table "%"',
            r.trigger_name, r.view_name, r.period_name, r.table_name;
    END LOOP;

    /* Complain if the table's primary key has been dropped. */
    FOR r IN
        SELECT fpv.table_name, fpv.period_name
        FROM periods.for_portion_views AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (fpv.table_name, 'p'))
    LOOP
        RAISE EXCEPTION 'cannot drop primary key on table "%" because it has a FOR PORTION OF view for period "%"',
            r.table_name, r.period_name;
    END LOOP;

    ---
    --- unique_keys
    ---

    /*
     * We don't need to protect the individual columns as long as we protect
     * the indexes.  PostgreSQL will make sure they stick around.
     */

    /* Complain if the indexes implementing our unique indexes are missing. */
    FOR r IN
        SELECT uk.key_name, uk.table_name, uk.unique_constraint
        FROM periods.unique_keys AS uk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (uk.table_name, uk.unique_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in period unique key "%"',
            r.unique_constraint, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT uk.key_name, uk.table_name, uk.exclude_constraint
        FROM periods.unique_keys AS uk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (uk.table_name, uk.exclude_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in period unique key "%"',
            r.exclude_constraint, r.table_name, r.key_name;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /* Complain if any of the triggers are missing */
    FOR r IN
        SELECT fk.key_name, fk.table_name, fk.fk_insert_trigger
        FROM periods.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_name, fk.fk_insert_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.fk_insert_trigger, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT fk.key_name, fk.table_name, fk.fk_update_trigger
        FROM periods.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_name, fk.fk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.fk_update_trigger, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT fk.key_name, uk.table_name, fk.uk_update_trigger
        FROM periods.foreign_keys AS fk
        JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_name, fk.uk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.uk_update_trigger, r.table_name, r.key_name;
    END LOOP;

    FOR r IN
        SELECT fk.key_name, uk.table_name, fk.uk_delete_trigger
        FROM periods.foreign_keys AS fk
        JOIN periods.unique_keys AS uk ON uk.key_name = fk.unique_key
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_name, fk.uk_delete_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in period foreign key "%"',
            r.uk_delete_trigger, r.table_name, r.key_name;
    END LOOP;

    ---
    --- system_versioning
    ---

    FOR r IN
        SELECT dobj.object_identity, sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = sv.history_table_name
        WHERE dobj.object_type = 'table'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop table "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.object_identity, r.table_name;
    END LOOP;

    FOR r IN
        SELECT dobj.object_identity, sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = sv.view_name
        WHERE dobj.object_type = 'view'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop view "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.object_identity, r.table_name;
    END LOOP;

    FOR r IN
        SELECT dobj.object_identity, sv.table_name
        FROM periods.system_versioning AS sv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])
        WHERE dobj.object_type = 'function'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop function "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.object_identity, r.table_name;
    END LOOP;
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
    save_search_path text;
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

    /* Check that our system versioning functions are still here */
    save_search_path := pg_catalog.current_setting('search_path');
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true);
    FOR r IN
        SELECT *
        FROM periods.system_versioning AS sv
        CROSS JOIN LATERAL UNNEST(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (fn)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_proc AS p
            WHERE p.oid::regprocedure::text = u.fn
        )
    LOOP
        RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.fn, r.table_name;
    END LOOP;
    PERFORM pg_catalog.set_config('search_path', save_search_path, true);

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
        JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;

    /* Check GRANTs */
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() AS ev_ddl
        WHERE ev_ddl.command_tag = 'GRANT')
    THEN
        FOR r IN
            SELECT *,
                   EXISTS (
                       SELECT
                       FROM pg_class AS _c
                       CROSS JOIN LATERAL aclexplode(COALESCE(_c.relacl, acldefault('r', _c.relowner))) AS _acl
                       WHERE _c.oid = objects.table_name
                         AND _acl.grantee = objects.grantee
                         AND _acl.privilege_type = 'SELECT'
                   ) AS on_base_table
            FROM (
                SELECT sv.table_name,
                       c.oid::regclass::text AS object_name,
                       c.relkind AS object_type,
                       acl.privilege_type,
                       acl.privilege_type AS base_privilege_type,
                       acl.grantee,
                       'h' AS history_or_portion
                FROM periods.system_versioning AS sv
                JOIN pg_class AS c ON c.oid IN (sv.history_table_name, sv.view_name)
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl

                UNION ALL

                SELECT fpv.table_name,
                       c.oid::regclass::text,
                       c.relkind,
                       acl.privilege_type,
                       acl.privilege_type,
                       acl.grantee,
                       'p' AS history_or_portion
                FROM periods.for_portion_views AS fpv
                JOIN pg_class AS c ON c.oid = fpv.view_name
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl

                UNION ALL

                SELECT sv.table_name,
                       p.oid::regprocedure::text,
                       'f',
                       acl.privilege_type,
                       'SELECT',
                       acl.grantee,
                       'h'
                FROM periods.system_versioning AS sv
                JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
                CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
            ) AS objects
            ORDER BY object_name, object_type, privilege_type
        LOOP
            IF
                r.history_or_portion = 'h' AND
                (r.object_type, r.privilege_type) NOT IN (('r', 'SELECT'), ('v', 'SELECT'), ('f', 'EXECUTE'))
            THEN
                RAISE EXCEPTION 'cannot grant % to "%"; history objects are read-only',
                    r.privilege_type, r.object_name;
            END IF;

            IF NOT r.on_base_table THEN
                RAISE EXCEPTION 'cannot grant % directly to "%"; grant % to "%" instead',
                    r.privilege_type, r.object_name, r.base_privilege_type, r.table_name;
            END IF;
        END LOOP;

        /* Propagate GRANTs */
        FOR cmd IN
            SELECT format('GRANT %s ON %s %s TO %s',
                          string_agg(DISTINCT privilege_type, ', '),
                          object_type,
                          string_agg(DISTINCT object_name, ', '),
                          string_agg(DISTINCT COALESCE(a.rolname, 'public'), ', '))
            FROM (
                SELECT 'TABLE' AS object_type,
                       hc.oid::regclass::text AS object_name,
                       'SELECT' AS privilege_type,
                       acl.grantee
                FROM periods.system_versioning AS sv
                JOIN pg_class AS c ON c.oid = sv.table_name
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
                JOIN pg_class AS hc ON hc.oid IN (sv.history_table_name, sv.view_name)
                WHERE acl.privilege_type = 'SELECT'
                  AND NOT has_table_privilege(acl.grantee, hc.oid, 'SELECT')

                UNION ALL

                SELECT 'TABLE',
                       fpc.oid::regclass::text,
                       acl.privilege_type,
                       acl.grantee
                FROM periods.for_portion_views AS fpv
                JOIN pg_class AS c ON c.oid = fpv.table_name
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
                JOIN pg_class AS fpc ON fpc.oid = fpv.view_name
                WHERE NOT has_table_privilege(acl.grantee, fpc.oid, acl.privilege_type)

                UNION ALL

                SELECT 'FUNCTION',
                       hp.oid::regprocedure::text,
                       'EXECUTE',
                       acl.grantee
                FROM periods.system_versioning AS sv
                JOIN pg_class AS c ON c.oid = sv.table_name
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
                JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
                WHERE acl.privilege_type = 'SELECT'
                  AND NOT has_function_privilege(acl.grantee, hp.oid, 'EXECUTE')
            ) AS objects
            LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
            GROUP BY object_type
        LOOP
            EXECUTE cmd;
        END LOOP;
    END IF;

    /* Check REVOKEs */
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() AS ev_ddl
        WHERE ev_ddl.command_tag = 'REVOKE')
    THEN
        FOR r IN
            SELECT sv.table_name,
                   hc.oid::regclass::text AS object_name,
                   acl.privilege_type,
                   acl.privilege_type AS base_privilege_type
            FROM periods.system_versioning AS sv
            JOIN pg_class AS c ON c.oid = sv.table_name
            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
            JOIN pg_class AS hc ON hc.oid IN (sv.history_table_name, sv.view_name)
            WHERE acl.privilege_type = 'SELECT'
              AND NOT EXISTS (
                SELECT
                FROM aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS _acl
                WHERE _acl.privilege_type = 'SELECT'
                  AND _acl.grantee = acl.grantee)

            UNION ALL

            SELECT fpv.table_name,
                   hc.oid::regclass::text,
                   acl.privilege_type,
                   acl.privilege_type
            FROM periods.for_portion_views AS fpv
            JOIN pg_class AS c ON c.oid = fpv.table_name
            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
            JOIN pg_class AS hc ON hc.oid = fpv.view_name
            WHERE NOT EXISTS (
                SELECT
                FROM aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS _acl
                WHERE _acl.privilege_type = acl.privilege_type
                  AND _acl.grantee = acl.grantee)

            UNION ALL

            SELECT sv.table_name,
                   hp.oid::regprocedure::text,
                   'EXECUTE',
                   'SELECT'
            FROM periods.system_versioning AS sv
            JOIN pg_class AS c ON c.oid = sv.table_name
            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
            JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
            WHERE acl.privilege_type = 'SELECT'
              AND NOT EXISTS (
                SELECT
                FROM aclexplode(COALESCE(hp.proacl, acldefault('f', hp.proowner))) AS _acl
                WHERE _acl.privilege_type = 'EXECUTE'
                  AND _acl.grantee = acl.grantee)

            ORDER BY table_name, object_name
        LOOP
            RAISE EXCEPTION 'cannot revoke % directly from "%", revoke % from "%" instead',
                r.privilege_type, r.object_name, r.base_privilege_type, r.table_name;
        END LOOP;

        /* Propagate REVOKEs */
        FOR cmd IN
            SELECT format('REVOKE %s ON %s %s FROM %s',
                          string_agg(DISTINCT privilege_type, ', '),
                          object_type,
                          string_agg(DISTINCT object_name, ', '),
                          string_agg(DISTINCT COALESCE(a.rolname, 'public'), ', '))
            FROM (
                SELECT 'TABLE' AS object_type,
                       hc.oid::regclass::text AS object_name,
                       'SELECT' AS privilege_type,
                       hacl.grantee
                FROM periods.system_versioning AS sv
                JOIN pg_class AS hc ON hc.oid IN (sv.history_table_name, sv.view_name)
                CROSS JOIN LATERAL aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS hacl
                WHERE hacl.privilege_type = 'SELECT'
                  AND NOT has_table_privilege(hacl.grantee, sv.table_name, 'SELECT')

                UNION ALL

                SELECT 'TABLE' AS object_type,
                       hc.oid::regclass::text AS object_name,
                       hacl.privilege_type,
                       hacl.grantee
                FROM periods.for_portion_views AS fpv
                JOIN pg_class AS hc ON hc.oid = fpv.view_name
                CROSS JOIN LATERAL aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS hacl
                WHERE NOT has_table_privilege(hacl.grantee, fpv.table_name, hacl.privilege_type)

                UNION ALL

                SELECT 'FUNCTION' AS object_type,
                       hp.oid::regprocedure::text AS object_name,
                       'EXECUTE' AS privilege_type,
                       hacl.grantee
                FROM periods.system_versioning AS sv
                JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
                CROSS JOIN LATERAL aclexplode(COALESCE(hp.proacl, acldefault('f', hp.proowner))) AS hacl
                WHERE hacl.privilege_type = 'EXECUTE'
                  AND NOT has_table_privilege(hacl.grantee, sv.table_name, 'SELECT')
            ) AS objects
            LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
            GROUP BY object_type
        LOOP
            EXECUTE cmd;
        END LOOP;
    END IF;
END;
$function$;

