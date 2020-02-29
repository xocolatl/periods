/* Tests for access control on the history tables */

CREATE ROLE periods_acl_1;
CREATE ROLE periods_acl_2;
CREATE ROLE periods_acl_3;

/* OWNER */

-- We call this query several times, so make it a view for eaiser maintenance
CREATE VIEW show_owners AS
    SELECT c.relnamespace::regnamespace AS schema_name,
           c.relname AS object_name,
           CASE c.relkind
               WHEN 'r' THEN 'table'
               WHEN 'v' THEN 'view'
           END AS object_type,
           c.relowner::regrole AS owner
    FROM pg_class AS c
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relname = ANY (ARRAY['owner_test', 'owner_test_history', 'owner_test_with_history', 'owner_test__for_portion_of_p'])
    UNION ALL
    SELECT p.pronamespace, p.proname, 'function', p.proowner
    FROM pg_proc AS p
    WHERE p.pronamespace = 'public'::regnamespace
      AND p.proname = ANY (ARRAY['owner_test__as_of', 'owner_test__between', 'owner_test__between_symmetric', 'owner_test__from_to']);

CREATE TABLE owner_test (col text PRIMARY KEY, s integer, e integer);
ALTER TABLE owner_test OWNER TO periods_acl_1;
SELECT periods.add_period('owner_test', 'p', 's', 'e');
SELECT periods.add_for_portion_view('owner_test', 'p');
SELECT periods.add_system_time_period('owner_test');
SELECT periods.add_system_versioning('owner_test');
TABLE show_owners ORDER BY object_name;

-- This should change everything
ALTER TABLE owner_test OWNER TO periods_acl_2;
TABLE show_owners ORDER BY object_name;

-- These should change nothing
ALTER TABLE owner_test_history OWNER TO periods_acl_3;
ALTER VIEW owner_test_with_history OWNER TO periods_acl_3;
ALTER FUNCTION owner_test__as_of(timestamp with time zone) OWNER TO periods_acl_3;
ALTER FUNCTION owner_test__between(timestamp with time zone, timestamp with time zone) OWNER TO periods_acl_3;
ALTER FUNCTION owner_test__between_symmetric(timestamp with time zone, timestamp with time zone) OWNER TO periods_acl_3;
ALTER FUNCTION owner_test__from_to(timestamp with time zone, timestamp with time zone) OWNER TO periods_acl_3;
TABLE show_owners ORDER BY object_name;

-- This should put the owner back to the base table's owner
SELECT periods.drop_system_versioning('owner_test');
ALTER TABLE owner_test_history OWNER TO periods_acl_3;
TABLE show_owners ORDER BY object_name;
SELECT periods.add_system_versioning('owner_test');
TABLE show_owners ORDER BY object_name;

SELECT periods.drop_system_versioning('owner_test', drop_behavior => 'CASCADE', purge => true);
SELECT periods.drop_for_portion_view('owner_test', NULL);
DROP TABLE owner_test CASCADE;
DROP VIEW show_owners;

/* Clean up */

DROP ROLE periods_acl_1;
DROP ROLE periods_acl_2;
DROP ROLE periods_acl_3;
