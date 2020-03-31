SELECT setting::integer < 90600 AS pre_96
FROM pg_settings WHERE name = 'server_version_num';

/* Run tests as unprivileged user */
SET ROLE TO periods_unprivileged_user;

/* SYSTEM_TIME with date */

BEGIN;
SELECT transaction_timestamp()::date AS xd,
       transaction_timestamp()::timestamp AS xts,
       transaction_timestamp() AS xtstz
\gset

CREATE TABLE sysver_date (val text, start_date date, end_date date);
SELECT periods.add_system_time_period('sysver_date', 'start_date', 'end_date');
TABLE periods.periods;
INSERT INTO sysver_date DEFAULT VALUES;
SELECT val, start_date = :'xd' AS start_date_eq, end_date FROM sysver_date;
DROP TABLE sysver_date;

/* SYSTEM_TIME with timestamp without time zone */

CREATE TABLE sysver_ts (val text, start_ts timestamp without time zone, end_ts timestamp without time zone);
SELECT periods.add_system_time_period('sysver_ts', 'start_ts', 'end_ts');
TABLE periods.periods;
INSERT INTO sysver_ts DEFAULT VALUES;
SELECT val, start_ts = :'xts' AS start_ts_eq, end_ts FROM sysver_ts;
DROP TABLE sysver_ts;

/* SYSTEM_TIME with timestamp with time zone */

CREATE TABLE sysver_tstz (val text, start_tstz timestamp with time zone, end_tstz timestamp with time zone);
SELECT periods.add_system_time_period('sysver_tstz', 'start_tstz', 'end_tstz');
TABLE periods.periods;
INSERT INTO sysver_tstz DEFAULT VALUES;
SELECT val, start_tstz = :'xtstz' AS start_tstz_eq, end_tstz FROM sysver_tstz;
DROP TABLE sysver_tstz;

COMMIT;


/* Basic SYSTEM_TIME periods with CASCADE/purge */

CREATE TABLE sysver (val text);
SELECT periods.add_system_time_period('sysver', 'startname');
SELECT periods.drop_period('sysver', 'system_time', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver', end_column_name => 'endname');
SELECT periods.drop_period('sysver', 'system_time', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver', 'startname', 'endname');
TABLE periods.periods;
TABLE periods.system_time_periods;
SELECT periods.drop_system_time_period('sysver', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver', 'endname', 'startname',
        bounds_check_constraint => 'b',
        infinity_check_constraint => 'i',
        generated_always_trigger => 'g',
        write_history_trigger => 'w',
        truncate_trigger => 't');
TABLE periods.periods;
TABLE periods.system_time_periods;
SELECT periods.drop_system_time_period('sysver', drop_behavior => 'CASCADE', purge => true);
SELECT periods.add_system_time_period('sysver');
DROP TABLE sysver;
TABLE periods.periods;
TABLE periods.system_time_periods;


/* Forbid UNIQUE keys on system_time columns */
CREATE TABLE no_unique (col1 timestamp with time zone, s bigint, e bigint);
SELECT periods.add_period('no_unique', 'p', 's', 'e');
SELECT periods.add_unique_key('no_unique', ARRAY['col1'], 'p'); -- passes
SELECT periods.add_system_time_period('no_unique');
SELECT periods.add_unique_key('no_unique', ARRAY['system_time_start'], 'p'); -- fails
SELECT periods.add_unique_key('no_unique', ARRAY['system_time_end'], 'p'); -- fails
SELECT periods.add_unique_key('no_unique', ARRAY['col1'], 'system_time'); -- fails
SELECT periods.drop_system_time_period('no_unique');
SELECT periods.add_unique_key('no_unique', ARRAY['system_time_start'], 'p'); -- passes
SELECT periods.add_unique_key('no_unique', ARRAY['system_time_end'], 'p'); -- passes
SELECT periods.add_system_time_period('no_unique'); -- fails
SELECT periods.drop_unique_key('no_unique', 'no_unique_system_time_start_p');
SELECT periods.drop_unique_key('no_unique', 'no_unique_system_time_end_p');
/* Forbid foreign keys on system_time columns */
CREATE TABLE no_unique_ref (LIKE no_unique);
SELECT periods.add_period('no_unique_ref', 'q', 's', 'e');
SELECT periods.add_system_time_period('no_unique_ref');
SELECT periods.add_foreign_key('no_unique_ref', ARRAY['system_time_start'], 'q', 'no_unique_col1_p'); -- fails
SELECT periods.add_foreign_key('no_unique_ref', ARRAY['system_time_end'], 'q', 'no_unique_col1_p'); -- fails
SELECT periods.add_foreign_key('no_unique_ref', ARRAY['col1'], 'system_time', 'no_unique_col1_p'); -- fails
SELECT periods.drop_system_time_period('no_unique_ref');
SELECT periods.add_foreign_key('no_unique_ref', ARRAY['system_time_start'], 'q', 'no_unique_col1_p'); -- passes
SELECT periods.add_foreign_key('no_unique_ref', ARRAY['system_time_end'], 'q', 'no_unique_col1_p'); -- passes
SELECT periods.add_system_time_period('no_unique_ref'); -- fails
DROP TABLE no_unique, no_unique_ref;
