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
