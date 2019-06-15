\set ON_ERROR_STOP 0
/* Set up a sandbox (too bad if there's a real schema named "periods_tests" */
DROP SCHEMA IF EXISTS periods_tests CASCADE;
CREATE SCHEMA periods_tests;

/* Here we hope btree_gist is installed in public */
SET search_path = periods_tests, periods, public;


/*
 * Test creating a table, dropping a column, and then dropping the whole thing;
 * without any periods.
 */
CREATE TABLE beeswax (col1 text, col2 date);
ALTER TABLE beeswax DROP COLUMN col1;
DROP TABLE beeswax;


/* Basic period definitions with dates */

CREATE TABLE basic (val text, s date, e date);
TABLE periods.periods;
SELECT periods.add_period('basic', 'bp', 's', 'e');
TABLE periods.periods;
SELECT periods.drop_period('basic', 'bp');
TABLE periods.periods;
SELECT periods.add_period('basic', 'bp', 's', 'e');
TABLE periods.periods;
/* Test constraints */
INSERT INTO basic (val, s, e) VALUES ('x', null, null); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '3000-01-01', null); --fail
INSERT INTO basic (val, s, e) VALUES ('x', null, '1000-01-01'); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '3000-01-01', '1000-01-01'); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '1000-01-01', '3000-01-01'); --success
TABLE basic;
/* Test dropping the whole thing */
DROP TABLE basic;
TABLE periods.periods;


/* Basic SYSTEM_TIME periods with CASCADE/purge */

CREATE TABLE sysver (val text);
SELECT periods.add_system_time_period('sysver', 'startname');
SELECT periods.drop_period('sysver', 'system_time', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver', end_column_name => 'endname');
SELECT periods.drop_period('sysver', 'system_time', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver', 'startname', 'endname');
SELECT periods.drop_system_time_period('sysver', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver', 'endname', 'startname');
SELECT periods.drop_system_time_period('sysver', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver');
\d+ sysver
DROP TABLE sysver;


/* Clean up */
DROP SCHEMA periods_tests CASCADE;
RESET search_path;
