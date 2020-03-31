/*
 * An alternative file for pre-v12 is necessary because LEAST() and GREATEST()
 * were not constant folded.  It was actually while writing this extension that
 * the lack of optimization was noticed, and subsequently fixed.
 *
 * https://www.postgresql.org/message-id/flat/c6e8504c-4c43-35fa-6c8f-3c0b80a912cc%402ndquadrant.com
 */

SELECT setting::integer < 120000 AS pre_12
FROM pg_settings WHERE name = 'server_version_num';

/* Run tests as unprivileged user */
SET ROLE TO periods_unprivileged_user;

/* Basic SYSTEM VERSIONING */

CREATE TABLE sysver (val text, flap boolean);
SELECT periods.add_system_time_period('sysver', excluded_column_names => ARRAY['flap']);
TABLE periods.system_time_periods;
TABLE periods.system_versioning;
SELECT periods.add_system_versioning('sysver',
    history_table_name => 'custom_history_name',
    view_name => 'custom_view_name',
    function_as_of_name => 'custom_as_of',
    function_between_name => 'custom_between',
    function_between_symmetric_name => 'custom_between_symmetric',
    function_from_to_name => 'custom_from_to');
TABLE periods.system_versioning;
SELECT periods.drop_system_versioning('sysver', drop_behavior => 'CASCADE');
DROP TABLE custom_history_name;
SELECT periods.add_system_versioning('sysver');
TABLE periods.system_versioning;

INSERT INTO sysver (val, flap) VALUES ('hello', false);
SELECT val FROM sysver;
SELECT val FROM sysver_history ORDER BY system_time_start;

SELECT transaction_timestamp() AS ts1 \gset

UPDATE sysver SET val = 'world';
SELECT val FROM sysver;
SELECT val FROM sysver_history ORDER BY system_time_start;

UPDATE sysver SET flap = not flap;
UPDATE sysver SET flap = not flap;
UPDATE sysver SET flap = not flap;
UPDATE sysver SET flap = not flap;
UPDATE sysver SET flap = not flap;
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

/* Ensure functions are inlined */

SET TimeZone = 'UTC';
SET DateStyle = 'ISO';
EXPLAIN (COSTS OFF) SELECT * FROM sysver__as_of('2000-01-01');
EXPLAIN (COSTS OFF) SELECT * FROM sysver__from_to('1000-01-01', '3000-01-01');
EXPLAIN (COSTS OFF) SELECT * FROM sysver__between('1000-01-01', '3000-01-01');
EXPLAIN (COSTS OFF) SELECT * FROM sysver__between_symmetric('3000-01-01', '1000-01-01');

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
