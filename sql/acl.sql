SELECT CASE
    WHEN setting::integer >= 170000 THEN '17 ..'::text
    WHEN setting::integer >= 110000 THEN '11 .. 16'
    WHEN setting::integer >= 90600 THEN '9.6 .. 10'
    ELSE '.. 9.5' END
FROM pg_settings WHERE name = 'server_version_num';

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

/* FOR PORTION OF ACL */

-- We call this query several times, so make it a view for eaiser maintenance
CREATE VIEW show_acls AS
    SELECT row_number() OVER (ORDER BY array_position(ARRAY['table', 'view', 'function'], object_type),
                                       schema_name, object_name, grantee, privilege_type) AS sort_order,
           *
    FROM (
        SELECT c.relnamespace::regnamespace AS schema_name,
               c.relname AS object_name,
               CASE c.relkind
                   WHEN 'r' THEN 'table'
                   WHEN 'v' THEN 'view'
               END AS object_type,
               acl.grantee::regrole::text AS grantee,
               acl.privilege_type
        FROM pg_class AS c
        CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
        WHERE c.relname IN ('fpacl', 'fpacl__for_portion_of_p')
    ) AS _;

CREATE TABLE fpacl (col text PRIMARY KEY, s integer, e integer);
ALTER TABLE fpacl OWNER TO periods_acl_1;
SELECT periods.add_period('fpacl', 'p', 's', 'e');
SELECT periods.add_for_portion_view('fpacl', 'p');
TABLE show_acls ORDER BY sort_order;

GRANT SELECT, UPDATE ON TABLE fpacl__for_portion_of_p TO periods_acl_2; -- fail
GRANT SELECT, UPDATE ON TABLE fpacl TO periods_acl_2;
TABLE show_acls ORDER BY sort_order;

REVOKE UPDATE ON TABLE fpacl__for_portion_of_p FROM periods_acl_2; -- fail
REVOKE UPDATE ON TABLE fpacl FROM periods_acl_2;
TABLE show_acls ORDER BY sort_order;

SELECT periods.drop_for_portion_view('fpacl', 'p');
DROP TABLE fpacl CASCADE;
DROP VIEW show_acls;

/* History ACL */

-- We call this query several times, so make it a view for eaiser maintenance
CREATE VIEW show_acls AS
    SELECT row_number() OVER (ORDER BY array_position(ARRAY['table', 'view', 'function'], object_type),
                                       schema_name, object_name, grantee, privilege_type) AS sort_order,
           *
    FROM (
        SELECT c.relnamespace::regnamespace AS schema_name,
               c.relname AS object_name,
               CASE c.relkind
                   WHEN 'r' THEN 'table'
                   WHEN 'v' THEN 'view'
               END AS object_type,
               acl.grantee::regrole::text AS grantee,
               acl.privilege_type
        FROM pg_class AS c
        CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
        WHERE c.relname IN ('histacl', 'histacl_history', 'histacl_with_history')

        UNION ALL

        SELECT p.pronamespace::regnamespace,
               p.proname,
               'function',
               acl.grantee::regrole::text,
               acl.privilege_type
        FROM pg_proc AS p
        CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
        WHERE p.proname IN ('histacl__as_of', 'histacl__between', 'histacl__between_symmetric', 'histacl__from_to')
    ) AS _;

CREATE TABLE histacl (col text);
ALTER TABLE histacl OWNER TO periods_acl_1;
SELECT periods.add_system_time_period('histacl');
SELECT periods.add_system_versioning('histacl');
TABLE show_acls ORDER BY sort_order;

-- Disconnect, add some privs to the history table, and reconnect
SELECT periods.drop_system_versioning('histacl');
GRANT ALL ON TABLE histacl_history TO periods_acl_3;
TABLE show_acls ORDER BY sort_order;
SELECT periods.add_system_versioning('histacl');
TABLE show_acls ORDER BY sort_order;

-- These next 6 blocks should fail
GRANT ALL ON TABLE histacl_history TO periods_acl_3; -- fail
GRANT SELECT ON TABLE histacl_history TO periods_acl_3; -- fail
REVOKE ALL ON TABLE histacl_history FROM periods_acl_1; -- fail
TABLE show_acls ORDER BY sort_order;

GRANT ALL ON TABLE histacl_with_history TO periods_acl_3; -- fail
GRANT SELECT ON TABLE histacl_with_history TO periods_acl_3; -- fail
REVOKE ALL ON TABLE histacl_with_history FROM periods_acl_1; -- fail
TABLE show_acls ORDER BY sort_order;

GRANT ALL ON FUNCTION histacl__as_of(timestamp with time zone) TO periods_acl_3; -- fail
GRANT EXECUTE ON FUNCTION histacl__as_of(timestamp with time zone) TO periods_acl_3; -- fail
REVOKE ALL ON FUNCTION histacl__as_of(timestamp with time zone) FROM periods_acl_1; -- fail
TABLE show_acls ORDER BY sort_order;

GRANT ALL ON FUNCTION histacl__between(timestamp with time zone, timestamp with time zone) TO periods_acl_3; -- fail
GRANT EXECUTE ON FUNCTION histacl__between(timestamp with time zone, timestamp with time zone) TO periods_acl_3; -- fail
REVOKE ALL ON FUNCTION histacl__between(timestamp with time zone, timestamp with time zone) FROM periods_acl_1; -- fail
TABLE show_acls ORDER BY sort_order;

GRANT ALL ON FUNCTION histacl__between_symmetric(timestamp with time zone, timestamp with time zone) TO periods_acl_3; -- fail
GRANT EXECUTE ON FUNCTION histacl__between_symmetric(timestamp with time zone, timestamp with time zone) TO periods_acl_3; -- fail
REVOKE ALL ON FUNCTION histacl__between_symmetric(timestamp with time zone, timestamp with time zone) FROM periods_acl_1; -- fail
TABLE show_acls ORDER BY sort_order;

GRANT ALL ON FUNCTION histacl__from_to(timestamp with time zone, timestamp with time zone) TO periods_acl_3; -- fail
GRANT EXECUTE ON FUNCTION histacl__from_to(timestamp with time zone, timestamp with time zone) TO periods_acl_3; -- fail
REVOKE ALL ON FUNCTION histacl__from_to(timestamp with time zone, timestamp with time zone) FROM periods_acl_1; -- fail
TABLE show_acls ORDER BY sort_order;

-- This one should work and propagate
GRANT ALL ON TABLE histacl TO periods_acl_2;
TABLE show_acls ORDER BY sort_order;
REVOKE SELECT ON TABLE histacl FROM periods_acl_2;
TABLE show_acls ORDER BY sort_order;

SELECT periods.drop_system_versioning('histacl', drop_behavior => 'CASCADE', purge => true);
DROP TABLE histacl CASCADE;
DROP VIEW show_acls;

/* Who can modify the history table? */

CREATE TABLE retention (value integer);
ALTER TABLE retention OWNER TO periods_acl_1;
REVOKE ALL ON TABLE retention FROM PUBLIC;
GRANT ALL ON TABLE retention TO periods_acl_2;
GRANT SELECT ON TABLE retention TO periods_acl_3;
SELECT periods.add_system_time_period('retention');
SELECT periods.add_system_versioning('retention');

INSERT INTO retention (value) VALUES (1);
UPDATE retention SET value = 2;

SET ROLE TO periods_acl_3;
DELETE FROM retention_history; -- fail
SET ROLE TO periods_acl_2;
DELETE FROM retention_history; -- fail
SET ROLE TO periods_acl_1;
DELETE FROM retention_history; -- fail

-- test what the docs say to do
BEGIN;
SELECT periods.drop_system_versioning('retention');
GRANT DELETE ON TABLE retention_history TO CURRENT_USER;
DELETE FROM retention_history;
SELECT periods.add_system_versioning('retention');
COMMIT;

-- superuser can do anything
RESET ROLE;
DELETE FROM retention_history;

SELECT periods.drop_system_versioning('retention', drop_behavior => 'CASCADE', purge => true);
DROP TABLE retention CASCADE;

/* Clean up */

DROP ROLE periods_acl_1;
DROP ROLE periods_acl_2;
DROP ROLE periods_acl_3;
