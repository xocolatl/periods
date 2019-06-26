/* Install the extension */
CREATE EXTENSION periods CASCADE;

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
TABLE periods.periods;

/* Basic SYSTEM VERSIONING */

CREATE TABLE sysver (val text);
SELECT periods.add_system_time_period('sysver');
TABLE periods.system_versioning;
SELECT periods.add_system_versioning('sysver');
TABLE periods.system_versioning;

INSERT INTO sysver (val) VALUES ('hello');
SELECT val FROM sysver;
SELECT val FROM sysver_history ORDER BY system_time_start;

SELECT transaction_timestamp() AS ts1 \gset

UPDATE sysver SET val = 'world';
SELECT val FROM sysver;
SELECT val FROM sysver_history ORDER BY system_time_start;

SELECT transaction_timestamp() AS ts2 \gset

DELETE FROM sysver;
SELECT val FROM sysver;
SELECT val FROM sysver_history ORDER BY system_time_start;

/* temporal queries */

SELECT val FROM sysver__as_of(:'ts1') ORDER BY system_time_start;
SELECT val FROM sysver__as_of(:'ts2') ORDER BY system_time_start;

SELECT val FROM sysver__from_to(:'ts1', :'ts2') ORDER BY system_time_start;
SELECT val FROM sysver__from_to(:'ts2', :'ts1') ORDER BY system_time_start;

SELECT val FROM sysver__between(:'ts1', :'ts2') ORDER BY system_time_start;
SELECT val FROM sysver__between(:'ts2', :'ts1') ORDER BY system_time_start;

SELECT val FROM sysver__between_symmetric(:'ts1', :'ts2') ORDER BY system_time_start;
SELECT val FROM sysver__between_symmetric(:'ts2', :'ts1') ORDER BY system_time_start;

-- We can't drop the the table without first dropping SYSTEM VERSIONING because
-- Postgres will complain about dependant objects (our view functions) before
-- we get a chance to clean them up.
SELECT periods.drop_system_versioning('sysver');
TABLE periods.system_versioning;
DROP TABLE sysver;
TABLE periods.periods;
\d


/* Clean up */
DROP EXTENSION periods;
