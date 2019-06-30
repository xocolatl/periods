SELECT setting::integer < 90600 AS pre_96 FROM pg_settings WHERE name = 'server_version_num';

/* Install the extension */
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION periods;

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


/* Unique and Foreign Keys */

-- Unique keys are already pretty much guaranteed by the underlying features of
-- PostgreSQL, but test them anyway.
CREATE TABLE uk (id integer, s integer, e integer, CONSTRAINT uk_pkey PRIMARY KEY (id, s, e));
SELECT periods.add_period('uk', 'p', 's', 'e');
SELECT periods.add_unique_key('uk', ARRAY['id'], 'p', key_name => 'uk_id_p', unique_constraint => 'uk_pkey');
INSERT INTO uk (id, s, e) VALUES (100, 1, 3), (100, 3, 4), (100, 4, 10); -- success
INSERT INTO uk (id, s, e) VALUES (200, 1, 3), (200, 3, 4), (200, 5, 10); -- success
INSERT INTO uk (id, s, e) VALUES (300, 1, 3), (300, 3, 5), (300, 4, 10); -- fail

CREATE TABLE fk (id integer, uk_id integer, s integer, e integer, PRIMARY KEY (id));
SELECT periods.add_period('fk', 'q', 's', 'e');
SELECT periods.add_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p', key_name => 'fk_uk_id_q');
-- INSERT
INSERT INTO fk VALUES (0, 100, 0, 1); -- fail
INSERT INTO fk VALUES (0, 100, 0, 10); -- fail
INSERT INTO fk VALUES (0, 100, 1, 11); -- fail
INSERT INTO fk VALUES (1, 100, 1, 3); -- success
INSERT INTO fk VALUES (2, 100, 1, 10); -- success
-- UPDATE
UPDATE fk SET e = 20 WHERE id = 1; -- fail
UPDATE fk SET e = 6 WHERE id = 1; -- success
UPDATE uk SET s = 2 WHERE (id, s, e) = (100, 1, 3); -- fail
UPDATE uk SET s = 0 WHERE (id, s, e) = (100, 1, 3); -- success
-- DELETE
DELETE FROM uk WHERE (id, s, e) = (100, 3, 4); -- fail
DELETE FROM uk WHERE (id, s, e) = (200, 3, 5); -- success

DROP TABLE fk;
DROP TABLE uk;


/* FOR PORTION tests */

CREATE TABLE pricing (product text, min_quantity integer, max_quantity integer, price numeric);
SELECT periods.add_period('pricing', 'quantities', 'min_quantity', 'max_quantity');
SELECT periods.add_for_portion_view('pricing', 'quantities');
TABLE periods.for_portion_views;
/* Test UPDATE FOR PORTION */
INSERT INTO pricing VALUES ('Trinket', 1, 20, 100);
TABLE pricing ORDER BY min_quantity;
UPDATE pricing__for_portion_of_quantities SET min_quantity = 30, max_quantity = 50, price = 80;
TABLE pricing ORDER BY min_quantity;
UPDATE pricing__for_portion_of_quantities SET min_quantity = 10, max_quantity = 20, price = 80;
TABLE pricing ORDER BY min_quantity;
UPDATE pricing__for_portion_of_quantities SET min_quantity = 5, max_quantity = 15, price = 90;
TABLE pricing ORDER BY min_quantity;
-- If we drop the period (without CASCADE) then the FOR PORTION views should be
-- dropped, too.
SELECT periods.drop_period('pricing', 'quantities');
TABLE periods.for_portion_views;
-- Add it back to test the drop_for_portion_view function
SELECT periods.add_period('pricing', 'quantities', 'min_quantity', 'max_quantity');
SELECT periods.add_for_portion_view('pricing', 'quantities');
-- We can't drop the the table without first dropping the FOR PORTION views
-- because Postgres will complain about dependant objects (our views) before we
-- get a chance to clean them up.
DROP TABLE pricing;
SELECT periods.drop_for_portion_view('pricing', NULL);
TABLE periods.for_portion_views;
DROP TABLE pricing;


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
DROP TABLE sysver;
TABLE periods.periods;
TABLE periods.system_time_periods;

/* Basic SYSTEM VERSIONING */

CREATE TABLE sysver (val text);
SELECT periods.add_system_time_period('sysver');
TABLE periods.system_time_periods;
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

/* TRUNCATE should delete the history, too */
SELECT val FROM sysver_with_history;
TRUNCATE sysver;
SELECT val FROM sysver_with_history; --empty

/* Try modifying several times in a transaction */
BEGIN;
INSERT INTO sysver (val) VALUES ('hello');
INSERT INTO sysver (val) VALUES ('world');
ROLLBACK;
SELECT val FROM sysver_with_history; --empty

BEGIN;
INSERT INTO sysver (val) VALUES ('hello');
UPDATE sysver SET val = 'world';
UPDATE sysver SET val = 'world2';
UPDATE sysver SET val = 'world3';
DELETE FROM sysver;
COMMIT;
SELECT val FROM sysver_with_history; --empty

-- We can't drop the the table without first dropping SYSTEM VERSIONING because
-- Postgres will complain about dependant objects (our view functions) before
-- we get a chance to clean them up.
DROP TABLE sysver;
SELECT periods.drop_system_versioning('sysver', drop_behavior => 'CASCADE', purge => true);
TABLE periods.system_versioning;
DROP TABLE sysver;
TABLE periods.periods;
TABLE periods.system_time_periods;


/* Clean up */
DROP EXTENSION periods;
